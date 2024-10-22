package rewards

import "database/sql"

const stakerShareSnapshotsQuery = `
with staker_shares_with_block_info as (
	select
		ss.staker,
		ss.strategy,
		ss.shares,
		ss.block_number,
		b.block_time,
		DATE(b.block_time) as block_date
	from staker_shares as ss
	left join blocks as b on (b.number = ss.block_number)
	where
		DATE(b.block_time) >= DATE(@startDate)
		and DATE(b.block_time) < DATE(@cutoffDate)
),
ranked_staker_records as (
SELECT *,
	ROW_NUMBER() OVER (PARTITION BY staker, strategy, block_date ORDER BY block_time DESC) AS rn
FROM staker_shares_with_block_info
),
-- Get the latest record for each day & round up to the snapshot day
snapshotted_records as (
	SELECT
		staker,
		strategy,
		shares,
		block_time,
		DATE(block_date, '+1 day') as snapshot_time
	from ranked_staker_records
	where rn = 1
),
-- Get the range for each operator, strategy pairing
staker_share_windows as (
	SELECT
		staker, strategy, shares, snapshot_time as start_time,
		CASE
			-- If the range does not have the end, use the current timestamp truncated to 0 UTC
			WHEN LEAD(snapshot_time) OVER (PARTITION BY staker, strategy ORDER BY snapshot_time) is null THEN DATE(@cutoffDate)
			ELSE LEAD(snapshot_time) OVER (PARTITION BY staker, strategy ORDER BY snapshot_time)
		END AS end_time
	FROM snapshotted_records
),
cleaned_records as (
	SELECT * FROM staker_share_windows
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
	SELECT
		staker,
		strategy,
		shares,
		day as snapshot
	FROM cleaned_records
	cross join day_series
		where DATE(day) between DATE(start_time) and DATE(end_time, '-1 day')
)
select * from final_results
`

type StakerShareSnapshot struct {
	Staker   string
	Strategy string
	Snapshot string
	Shares   string
}

func (r *RewardsCalculator) GenerateStakerShareSnapshots(startDate string, snapshotDate string) ([]*StakerShareSnapshot, error) {
	results := make([]*StakerShareSnapshot, 0)

	res := r.grm.Raw(stakerShareSnapshotsQuery,
		sql.Named("startDate", startDate),
		sql.Named("cutoffDate", snapshotDate),
	).Scan(&results)

	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate staker share snapshots", "error", res.Error)
		return nil, res.Error
	}
	return results, nil
}

func (r *RewardsCalculator) GenerateAndInsertStakerShareSnapshots(startDate string, snapshotDate string) error {
	snapshots, err := r.GenerateStakerShareSnapshots(startDate, snapshotDate)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate staker share snapshots", "error", err)
		return err
	}

	r.logger.Sugar().Infow("Inserting staker share snapshots", "count", len(snapshots))
	res := r.grm.Model(&StakerShareSnapshot{}).CreateInBatches(snapshots, 100)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to insert staker share snapshots", "error", res.Error)
		return res.Error
	}
	return nil
}

func (r *RewardsCalculator) CreateStakerShareSnapshotsTable() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS staker_share_snapshots (
			staker TEXT,
			strategy TEXT,
			shares TEXT,
			snapshot TEXT
		)
		`,
		`create index idx_staker_share_snapshots_staker_strategy_snapshot on staker_share_snapshots (staker, strategy, snapshot)`,
		`create index idx_staker_share_snapshots_strategy_snapshot on staker_share_snapshots (strategy, snapshot)`,
	}
	for _, query := range queries {
		res := r.grm.Exec(query)
		if res.Error != nil {
			r.logger.Sugar().Errorw("Failed to create staker share snapshots table", "error", res.Error)
			return res.Error
		}
	}
	return nil
}
