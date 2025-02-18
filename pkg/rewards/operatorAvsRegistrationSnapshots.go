package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
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
insert into operator_avs_registration_snapshots (operator, avs, snapshot)
WITH state_changes as (
	select
		aosc.*,
		b.block_time::timestamp(6) as block_time,
		to_char(b.block_time, 'YYYY-MM-DD') AS block_date
	from avs_operator_state_changes as aosc
	left join blocks as b on (b.number = aosc.block_number)
	-- pipeline bronze table uses this to filter the correct records
	where b.block_time < TIMESTAMP '{{.cutoffDate}}'
),
marked_statuses AS (
    SELECT
        operator,
        avs,
        registered,
        block_time,
        block_date,
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
 removed_same_day_deregistrations AS (
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
		COALESCE(next_block_time, '{{.cutoffDate}}')::timestamp AS end_time,
		registered
	FROM removed_same_day_deregistrations
	WHERE registered = TRUE
 ),
-- Round UP each start_time and round DOWN each end_time
registration_windows_extra as (
	SELECT
		operator,
		avs,
		date_trunc('day', start_time) + interval '1' day as start_time,
		-- End time is end time non inclusive becuase the operator is not registered on the AVS at the end time OR it is current timestamp rounded up
		date_trunc('day', end_time) as end_time
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
)
SELECT
	operator,
	avs,
	d AS snapshot
FROM cleaned_records
CROSS JOIN generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS d
on conflict on constraint uniq_operator_avs_registration_snapshots do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertOperatorAvsRegistrationSnapshots(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(operatorAvsRegistrationSnapshotsQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render operator AVS registration snapshots query", zap.Error(err))
		return err
	}

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate operator_avs_registration_snapshots",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) ListOperatorAvsRegistrationSnapshots() ([]*OperatorAvsRegistrationSnapshots, error) {
	var operatorAvsRegistrationSnapshots []*OperatorAvsRegistrationSnapshots
	res := rc.grm.Model(&OperatorAvsRegistrationSnapshots{}).Find(&operatorAvsRegistrationSnapshots)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list operator AVS registration snapshots", "error", res.Error)
		return nil, res.Error
	}
	return operatorAvsRegistrationSnapshots, nil
}
