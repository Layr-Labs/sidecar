package rewards

import (
	"database/sql"
)

// Operator AVS Registration Windows: Ranges at which an operator has registered for an AVS
// 0. Ranked: Rank the operator state changes by block_time and log_index since sqlite lacks LEAD/LAG functions
// 1. Marked_statuses: Denote which registration status comes after one another
// 2. Removed_same_day_deregistrations: Remove a pairs of (registration, deregistration) that happen on the same day
// 3. Registration_periods: Combine registration together, only select registrations with:
// a. (Registered, Unregistered)
// b. (Registered, Null). If null, the end time is the current timestamp
// 4. Registration_snapshots: Round up each start_time to  0 UTC on NEXT DAY and round down each end_time to 0 UTC on CURRENT DAY
// 5. Operator_avs_registration_windows: Ranges that start and end on same day are invalid
// Note: We cannot assume that the operator is registered for avs at end_time because it is
// Payments calculations should only be done on snapshots from the PREVIOUS DAY. For example say we have the following:
// <-----0-------1-------2------3------>
// ^           ^
// Entry        Exit
// Since exits (deregistrations) are rounded down, we must only look at the day 2 snapshot on a pipeline run on day 3.
const operatorAvsRegistrationSnapshotsQuery = `
WITH state_changes as (
	select
		aosc.*,
		b.block_time as block_time,
		DATE(b.block_time) as block_date
	from avs_operator_state_changes as aosc
	left join blocks as b on (b.number = aosc.block_number)
),
marked_statuses as (
	SELECT
		operator,
		avs,
		registered,
		block_time,
		DATE(block_time) as block_date,
		-- Mark the next action as next_block_time
		LEAD(block_time) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_block_time,
		-- The below lead/lag combinations are only used in the next CTE
		-- Get the next row's registered status and block_date
		LEAD(registered) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_registration_status,
		LEAD(block_date) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS next_block_date,
		-- Get the previous row's registered status and block_date
		LAG(registered) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS prev_registered,
		LAG(block_date) OVER (PARTITION BY operator, avs ORDER BY block_time ASC, log_index ASC) AS prev_block_date
	FROM state_changes
),
-- Ignore a (registration,deregistration) pairs that happen on the exact same date
removed_same_day_deregistrations as (
	SELECT * from marked_statuses
	WHERE NOT (
	-- Remove the registration part
		(registered = TRUE AND
			COALESCE(next_registration_status = FALSE, false) AND -- default to false if null
			COALESCE(block_date = next_block_date, false)) OR
 	-- Remove the deregistration part
		(registered = FALSE AND
			COALESCE(prev_registered = TRUE, false) and
			COALESCE(block_date = prev_block_date, false)
		)
	)
),
-- Combine corresponding registrations into a single record
-- start_time is the beginning of the record
registration_periods AS (
	SELECT
		operator,
		avs,
		block_time AS start_time,
		-- Mark the next_block_time as the end_time for the range
		-- Use coalesce because if the next_block_time for a registration is not closed, then we use cutoff_date
		COALESCE(next_block_time, DATETIME(@cutoffDate)) AS end_time,
		registered
	FROM removed_same_day_deregistrations
	WHERE registered = TRUE
),
-- Round UP each start_time and round DOWN each end_time
registration_windows_extra as (
	SELECT
		operator,
		avs,
		DATE(start_time, '+1 day') as start_time,
		-- End time is end time non inclusive because the operator is not registered on the AVS at the end time OR it is current timestamp rounded up
		DATE(end_time) as end_time
		FROM registration_periods
),
-- Ignore start_time and end_time that last less than a day
operator_avs_registration_windows as (
	SELECT * from registration_windows_extra
	WHERE start_time != end_time
),
cleaned_records AS (
	SELECT * FROM operator_avs_registration_windows
	WHERE start_time < end_time
),
date_bounds as (
	select
		min(start_time) as min_start,
		max(end_time) as max_end
	from operator_avs_registration_windows
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
		AND DATE(snapshot) < DATE(@cutoffDate)
`

type OperatorAvsRegistrationSnapshots struct {
	Avs      string
	Operator string
	Snapshot string
}

// GenerateOperatorAvsRegistrationSnapshots returns a list of OperatorAvsRegistrationSnapshots objects
func (r *RewardsCalculator) GenerateOperatorAvsRegistrationSnapshots(startDate string, snapshotDate string) ([]*OperatorAvsRegistrationSnapshots, error) {
	results := make([]*OperatorAvsRegistrationSnapshots, 0)

	res := r.grm.Raw(operatorAvsRegistrationSnapshotsQuery,
		sql.Named("startDate", startDate),
		sql.Named("cutoffDate", snapshotDate),
	).Scan(&results)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate operator AVS registration windows", "error", res.Error)
		return nil, res.Error
	}
	return results, nil
}

func (r *RewardsCalculator) GenerateAndInsertOperatorAvsRegistrationSnapshots(startDate string, snapshotDate string) error {
	snapshots, err := r.GenerateOperatorAvsRegistrationSnapshots(startDate, snapshotDate)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate operator AVS registration snapshots", "error", err)
		return err
	}

	r.logger.Sugar().Infow("Inserting operator AVS registration snapshots", "count", len(snapshots))

	res := r.grm.Model(&OperatorAvsRegistrationSnapshots{}).CreateInBatches(snapshots, 5000)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to insert operator AVS registration snapshots", "error", res.Error)
		return err
	}
	return nil
}

func (r *RewardsCalculator) CreateOperatorAvsRegistrationSnapshotsTable() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS operator_avs_registration_snapshots (
			avs TEXT,
			operator TEXT,
			snapshot TEXT
		)`,
		`CREATE INDEX IF NOT EXISTS idx_operator_avs_registration_snapshots_avs_snapshot ON operator_avs_registration_snapshots (avs, snapshot)`,
		// `CREATE INDEX IF NOT EXISTS idx_operator_avs_registration_snapshots_operator_snapshot ON operator_avs_registration_snapshots (operator, snapshot)`,
	}
	for _, query := range queries {
		res := r.grm.Exec(query)
		if res.Error != nil {
			r.logger.Sugar().Errorw("Failed to create operator_avs_registration_snapshots table", "error", res.Error)
			return res.Error
		}
	}

	return nil
}
