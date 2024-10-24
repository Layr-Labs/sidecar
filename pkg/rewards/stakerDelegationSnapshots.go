package rewards

import (
	"database/sql"
)

const stakerDelegationSnapshotsQuery = `
with staker_delegations_with_block_info as (
	select
		sdc.staker,
		case when sdc.delegated = false then '0x0000000000000000000000000000000000000000' else sdc.operator end as operator,
		sdc.log_index,
		sdc.block_number,
		b.block_time,
		DATE(b.block_time) as block_date
	from staker_delegation_changes as sdc
	left join blocks as b on (b.number = sdc.block_number)
),
ranked_staker_records as (
SELECT *,
	ROW_NUMBER() OVER (PARTITION BY staker, block_date ORDER BY block_time DESC, log_index desc) AS rn
FROM staker_delegations_with_block_info
),
-- Get the latest record for each day & round up to the snapshot day
snapshotted_records as (
	SELECT
		staker,
		operator,
		block_time,
		DATE(block_date, '+1 day') as snapshot_time
	from ranked_staker_records
	where rn = 1
),
-- Get the range for each operator, strategy pairing
staker_share_windows as (
	SELECT
		staker, operator, snapshot_time as start_time,
		CASE
			-- If the range does not have the end, use the current timestamp truncated to 0 UTC
			WHEN LEAD(snapshot_time) OVER (PARTITION BY staker ORDER BY snapshot_time) is null THEN DATE(@cutoffDate)
			ELSE LEAD(snapshot_time) OVER (PARTITION BY staker ORDER BY snapshot_time)
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
		operator,
		day as snapshot
	FROM cleaned_records
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

type StakerDelegationSnapshot struct {
	Staker   string
	Operator string
	Snapshot string
}

func (r *RewardsCalculator) GenerateStakerDelegationSnapshots(startDate string, snapshotDate string) ([]*StakerDelegationSnapshot, error) {
	results := make([]*StakerDelegationSnapshot, 0)

	res := r.grm.Raw(stakerDelegationSnapshotsQuery,
		sql.Named("startDate", startDate),
		sql.Named("cutoffDate", snapshotDate),
	).Scan(&results)

	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate staker delegation snapshots", "error", res.Error)
		return nil, res.Error
	}
	return results, nil
}

func (r *RewardsCalculator) GenerateAndInsertStakerDelegationSnapshots(startDate string, snapshotDate string) error {
	snapshots, err := r.GenerateStakerDelegationSnapshots(startDate, snapshotDate)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate staker delegation snapshots", "error", err)
		return err
	}

	r.logger.Sugar().Infow("Inserting staker delegation snapshots", "count", len(snapshots))
	res := r.grm.Model(&StakerDelegationSnapshot{}).CreateInBatches(snapshots, 5000)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to insert staker delegation snapshots", "error", res.Error)
		return res.Error
	}

	return nil
}

func (r *RewardsCalculator) CreateStakerDelegationSnapshotsTable() error {
	queries := []string{
		`
			CREATE TABLE IF NOT EXISTS staker_delegation_snapshots (
				staker TEXT,
				operator TEXT,
				snapshot TEXT
			)
		`,
		`create index idx_staker_delegation_snapshots_operator_snapshot on staker_delegation_snapshots (operator, snapshot)`,
		`create index idx_staker_delegation_snapshots_snapshot on staker_delegation_snapshots (snapshot)`,
	}

	for _, query := range queries {
		res := r.grm.Exec(query)
		if res.Error != nil {
			r.logger.Sugar().Errorw("Failed to create staker delegation snapshots table", "error", res.Error)
			return res.Error
		}
	}
	return nil
}
