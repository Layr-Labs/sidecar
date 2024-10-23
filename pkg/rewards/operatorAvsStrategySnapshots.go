package rewards

import (
	"database/sql"
)

// Operator AVS Strategy Windows: Ranges for which an Operator, Strategy is restaked on an AVS
// 1. Ranked_records: Order all records. Round up block_time to 0 UTC
// 2. Latest_records: Get latest records for each (operator, avs, strategy, day) combination
// 3. Grouped_records: Find the next start_time for each (operator, avs, strategy) combination
// 4. Parsed_ranges: Complicated step. Here, we set end_time = start_time in three cases:
// Case 1: end_time is null because there are no more RPC calls made. For example, if today is 4/28 and
// the start_time is 4/27 (rounded from 4/26), there is nothing we can do on the (4/27, 4/28)
// range since it has not ended.
// Case 2: end_time is null because the (operator, strategy) combo is no longer registered
// Case 3: end_time is more than 1 day greater than start_time. In this case, if there is a new range,
// it will be accounted for. Say we have a range (4/21, 4/22, 4/23), (4/23, 4/25), (4/25, 4/26).
// The second range will be discarded since its not contiguous. We will keep (4/21-4/23) and (4/25-4/26)
// 5. Active_windows: Parse out all rows whose start_time == end_time (see above conditions)
// 6. Gaps_and_islands: Mark the previous end time for each row. If null, then start of a range
// 7. Island_detection: Mark islands if the previous end time is equal to the start time
// 8. Island_groups: Group islands by summing up ids
// 9. Operator_avs_strategy_windows: Combine ranges with same id
const operatorAvsStrategyWindowsQuery = `
with ranked_records AS (
	SELECT
		lower(operator) as operator,
		lower(avs) as avs,
		lower(strategy) as strategy,
		block_time,
		DATE(block_time, '+1 day') as start_time,
		ROW_NUMBER() OVER (
			PARTITION BY operator, avs, strategy, DATE(block_time, '+1 day')
			ORDER BY block_time DESC -- want latest records to be ranked highest
		) AS rn
	FROM operator_restaked_strategies
	WHERE avs_directory_address = lower(@avsDirectoryAddress)
),
-- Get the latest records for each (operator, avs, strategy, day) combination
latest_records AS (
	SELECT
		operator,
		avs,
		strategy,
		start_time,
		block_time,
		rn
	FROM ranked_records
	WHERE rn = 1
),
-- Find the next entry for each (operator,avs,strategy) grouping
grouped_records AS (
    SELECT
        operator,
        avs,
        strategy,
        start_time,
        LEAD(start_time) OVER (
            PARTITION BY operator, avs, strategy
            ORDER BY start_time ASC
        ) AS next_start_time
    FROM latest_records
),
-- Parse out any holes (ie. any next_start_times that are not exactly one day after the current start_time)
parsed_ranges AS (
	SELECT
		operator,
		avs,
		strategy,
		start_time,
		-- If the next_start_time is not on the consecutive day, close off the end_time
		CASE
			WHEN next_start_time IS NULL OR next_start_time > DATE(start_time, '+1 day') THEN start_time
			ELSE next_start_time
		END AS end_time
	FROM grouped_records
),
-- Remove the (operator,avs,strategy) combos where start_time == end_time
active_windows as (
	SELECT *
	FROM parsed_ranges
	WHERE start_time != end_time
),
partitioned_gaps_and_islands as (
	select
		operator,
		avs,
		strategy,
		start_time,
		end_time,
		ROW_NUMBER() OVER (PARTITION BY operator, avs, strategy ORDER BY start_time) as rn
	from active_windows
),
-- Mark the prev_end_time for each row. If new window, then gap is empty
gaps_and_islands AS (
    SELECT
        operator,
        avs,
        strategy,
        start_time,
        end_time,
        LAG(end_time) OVER(PARTITION BY operator, avs, strategy ORDER BY start_time) as prev_end_time
    FROM active_windows
),
-- Detect islands
island_detection AS (
	SELECT operator, avs, strategy, start_time, end_time, prev_end_time,
		CASE
			-- If the previous end time is equal to the start time, then mark as part of the island, else create a new island
			WHEN prev_end_time = start_time THEN 0
			ELSE 1
			END as new_island
	FROM gaps_and_islands
),
-- Group each based on their ID
island_groups AS (
	SELECT
		t1.operator,
		t1.avs,
		t1.strategy,
		t1.start_time,
		t1.end_time,
		(
			SELECT SUM(t2.new_island)
			FROM island_detection t2
			WHERE t2.operator = t1.operator
			AND t2.avs = t1.avs
			AND t2.strategy = t1.strategy
			AND t2.start_time <= t1.start_time
		) AS island_id
	FROM island_detection t1
	ORDER BY t1.operator, t1.avs, t1.strategy, t1.start_time
),
operator_avs_strategy_windows AS (
	SELECT
		operator,
		avs,
		strategy,
		MIN(start_time) AS start_time,
		MAX(end_time) AS end_time
	FROM island_groups
	GROUP BY operator, avs, strategy, island_id
	ORDER BY operator, avs, strategy, start_time
),
cleaned_records AS (
	SELECT *
	FROM operator_avs_strategy_windows
	WHERE start_time < end_time
),
date_bounds as (
	select
		min(start_time) as min_start,
		max(end_time) as max_end
	from cleaned_records
),
day_series AS (
	with RECURSIVE day_series_inner AS (
		SELECT DATE(min_start) AS day
		FROM date_bounds
		UNION ALL
		SELECT DATE(day, '+1 day')
		FROM day_series_inner
		WHERE day < (SELECT max_end FROM date_bounds)
	)
	select * from day_series_inner
),
final_results as (
	select
		operator,
		avs,
		strategy,
		day as snapshot
	from cleaned_records
	cross join day_series
	where DATE(day) between DATE(start_time) and DATE(end_time, '-1 day')
)
select
	*
from final_results
where
	DATE(snapshot) >= DATE(@startDate)
	and DATE(snapshot) < DATE(@cutoffDate)
`

type OperatorAvsStrategySnapshot struct {
	Operator string
	Avs      string
	Strategy string
	Snapshot string
}

func (r *RewardsCalculator) GenerateOperatorAvsStrategySnapshots(startDate string, snapshotDate string) ([]*OperatorAvsStrategySnapshot, error) {
	results := make([]*OperatorAvsStrategySnapshot, 0)

	contractAddresses := r.globalConfig.GetContractsMapForChain()

	res := r.grm.Raw(operatorAvsStrategyWindowsQuery,
		sql.Named("avsDirectoryAddress", contractAddresses.AvsDirectory),
		sql.Named("startDate", startDate),
		sql.Named("cutoffDate", snapshotDate),
	).Scan(&results)

	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate operator AVS strategy windows", "error", res.Error)
		return nil, res.Error
	}
	return results, nil
}

func (r *RewardsCalculator) GenerateAndInsertOperatorAvsStrategySnapshots(startDate string, snapshotDate string) error {
	snapshots, err := r.GenerateOperatorAvsStrategySnapshots(startDate, snapshotDate)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate operator AVS strategy snapshots", "error", err)
		return err
	}

	r.logger.Sugar().Infow("Inserting operator AVS strategy snapshots", "count", len(snapshots))
	res := r.grm.Model(&OperatorAvsStrategySnapshot{}).CreateInBatches(snapshots, 100)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to insert operator AVS strategy snapshots", "error", res.Error)
		return res.Error
	}
	return nil
}

func (r *RewardsCalculator) CreateOperatorAvsStrategySnapshotsTable() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS operator_avs_strategy_snapshots (
				operator TEXT NOT NULL,
				avs TEXT NOT NULL,
				strategy TEXT NOT NULL,
				snapshot DATE NOT NULL
			);
		`,
		`CREATE INDEX IF NOT EXISTS idx_operator_avs_strategy_snapshots_avs_strat_snap ON operator_avs_strategy_snapshots (avs, strategy, snapshot);`,
	}
	for _, query := range queries {
		res := r.grm.Exec(query)
		if res.Error != nil {
			r.logger.Sugar().Errorw("Failed to create operator_avs_strategy_snapshots table", "error", res.Error)
			return res.Error
		}
	}
	return nil
}
