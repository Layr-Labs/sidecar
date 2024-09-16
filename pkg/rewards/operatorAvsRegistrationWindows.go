package rewards

import (
	"database/sql"
	"time"
)

type OperatorAvsRegistrationWindow struct {
	Avs       string
	Operator  string
	StartTime time.Time
	EndTime   time.Time
}

const query = `
WITH ranked as (
	select
		aosc.operator,
		aosc.avs,
		aosc.registered,
		aosc.log_index,
		aosc.block_number,
		b.block_time,
		DATE(b.block_date) as block_date,
		ROW_NUMBER() OVER (PARTITION BY aosc.operator, aosc.avs order by b.block_time asc, aosc.log_index asc) as rn
	from avs_operator_state_changes aosc
	left join blocks as b on (b.number = aosc.block_number)
),
marked_statuses as (
	select
		r.operator,
		r.avs,
		r.registered,
		r.block_number,
		r2.registered as next_registration_status,
		r2.block_number as next_block_number,
		r3.registered as prev_registered,
		r3.block_number as prev_block_number
	from ranked as r
	left join ranked as r2 on (r.operator = r2.operator and r.avs = r2.avs and r.rn = r2.rn - 1)
	left join ranked as r3 on (r.operator = r3.operator and r.avs = r3.avs and r.rn = r3.rn + 1)
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
)
select * from operator_avs_registration_windows
`

func (r *RewardsCalculator) GenerateOperatorAvsRegistrationWindows(snapshotDate string) ([]*OperatorAvsRegistrationWindow, error) {
	results := make([]*OperatorAvsRegistrationWindow, 0)

	res := r.grm.Raw(query, sql.Named("cutoffDate", snapshotDate)).Scan(&results)
	if res.Error != nil {
		return nil, res.Error
	}
	return results, nil
}
