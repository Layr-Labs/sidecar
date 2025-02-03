package rewards

import "github.com/Layr-Labs/sidecar/pkg/rewardsUtils"

// Operator Set Strategy Registration Windows: Ranges at which a strategy has been registered for an operator set by an AVS.
// 1. Marked_statuses: Denote which registration status comes after one another
// 2. Removed_same_day_deregistrations: Remove a pairs of (registration, deregistration) that happen on the same day
// 3. Registration_periods: Combine registration together, only select registrations with:
// a. (Registered, Unregistered)
// b. (Registered, Null). If null, the end time is the current timestamp
// 4. Registration_snapshots: Round up each start_time and end_time to 0 UTC on NEXT DAY.
// 5. Operator_set_strategy_registration_windows: Ranges that start and end on same day are invalid
const operatorSetStrategyRegistrationSnapshotsQuery = `
WITH state_changes as (
	select
		ossr.*,
		b.block_time::timestamp(6) as block_time,
		to_char(b.block_time, 'YYYY-MM-DD') AS block_date
	from operator_set_strategy_registrations as ossr
	join blocks as b on (b.number = ossr.block_number)
	where b.block_time < TIMESTAMP '{{.cutoffDate}}'
),
marked_statuses AS (
    SELECT
        strategy,
        avs,
		operator_set_id,
        is_active,
        block_time,
        block_date,
        -- Mark the next action as next_block_time
        LEAD(block_time) OVER (PARTITION BY strategy, avs, operator_set_id ORDER BY block_time ASC, log_index ASC) AS next_block_time,
        -- The below lead/lag combinations are only used in the next CTE
        -- Get the next row's registered status and block_date
        LEAD(is_active) OVER (PARTITION BY strategy, avs, operator_set_id ORDER BY block_time ASC, log_index ASC) AS next_is_active,
        LEAD(block_date) OVER (PARTITION BY strategy, avs, operator_set_id ORDER BY block_time ASC, log_index ASC) AS next_block_date,
        -- Get the previous row's registered status and block_date
        LAG(is_active) OVER (PARTITION BY strategy, avs, operator_set_id ORDER BY block_time ASC, log_index ASC) AS prev_is_active,
        LAG(block_date) OVER (PARTITION BY strategy, avs, operator_set_id ORDER BY block_time ASC, log_index ASC) AS prev_block_date
    FROM state_changes
),
-- Ignore a (registration,deregistration) pairs that happen on the exact same date
 removed_same_day_deregistrations AS (
	 SELECT * from marked_statuses
	 WHERE NOT (
		 -- Remove the registration part
		 (is_active = TRUE AND
		  COALESCE(next_is_active = FALSE, false) AND -- default to false if null
		  COALESCE(block_date = next_block_date, false)) OR
			 -- Remove the deregistration part
		 (is_active = FALSE AND
		  COALESCE(prev_is_active = TRUE, false) and
		  COALESCE(block_date = prev_block_date, false)
			 )
		 )
 ),
-- Combine corresponding registrations into a single record
-- start_time is the beginning of the record
 registration_periods AS (
	SELECT
		strategy,
		avs,
		operator_set_id,
		block_time AS start_time,
		-- Mark the next_block_time as the end_time for the range
		-- Use coalesce because if the next_block_time for a registration is not closed, then we use cutoff_date
		COALESCE(next_block_time, '{{.cutoffDate}}')::timestamp AS end_time,
		is_active
	FROM removed_same_day_deregistrations
	WHERE is_active = TRUE
 ),
-- Round UP both start_time and end_time, with generate_series handling the exclusive end
registration_windows_extra as (
	SELECT
		strategy,
		avs,
		operator_set_id,
		date_trunc('day', start_time) + interval '1' day as start_time,
		-- End time is rounded up to include the full last day of registration
		date_trunc('day', end_time) + interval '1' day as end_time
	FROM registration_periods
),
-- Ignore start_time and end_time that last less than a day
operator_set_strategy_registration_windows as (
	 SELECT * from registration_windows_extra
	 WHERE start_time != end_time
),
cleaned_records AS (
	SELECT * FROM operator_set_strategy_registration_windows
	WHERE start_time < end_time
)
SELECT
	strategy,
	avs,
	operator_set_id,
	d AS snapshot
FROM cleaned_records
CROSS JOIN generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS d
`

func (r *RewardsCalculator) GenerateAndInsertOperatorSetStrategyRegistrationSnapshots(snapshotDate string) error {
	tableName := "operator_set_strategy_registration_snapshots"

	query, err := rewardsUtils.RenderQueryTemplate(operatorSetStrategyRegistrationSnapshotsQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render operator set operator registration snapshots query", "error", err)
		return err
	}

	err = r.generateAndInsertFromQuery(tableName, query, nil)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate operator_set_strategy_registration_snapshots", "error", err)
		return err
	}
	return nil
}

func (rc *RewardsCalculator) ListOperatorSetStrategyRegistrationSnapshots() ([]*OperatorSetStrategyRegistrationSnapshots, error) {
	var operatorSetStrategyRegistrationSnapshots []*OperatorSetStrategyRegistrationSnapshots
	res := rc.grm.Model(&OperatorSetStrategyRegistrationSnapshots{}).Find(&operatorSetStrategyRegistrationSnapshots)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list operator set strategy registration snapshots", "error", res.Error)
		return nil, res.Error
	}
	return operatorSetStrategyRegistrationSnapshots, nil
}
