package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const stakerDelegationSnapshotsQuery = `
insert into staker_delegation_snapshots(staker, operator, snapshot)
with staker_delegations_with_block_info as (
	select
		sdc.staker,
		case when sdc.delegated = false then '0x0000000000000000000000000000000000000000' else sdc.operator end as operator,
		sdc.log_index,
		sdc.block_number,
		b.block_time::timestamp(6) as block_time,
		to_char(b.block_time, 'YYYY-MM-DD') AS block_date
	from staker_delegation_changes as sdc
	left join blocks as b on (b.number = sdc.block_number)
	-- pipeline bronze table uses this to filter the correct records
	where b.block_time < TIMESTAMP '{{.cutoffDate}}'
),
ranked_delegations as (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY staker, cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
    FROM staker_delegations_with_block_info
),
-- Get the latest record for each day & round up to the snapshot day
snapshotted_records as (
 SELECT
	 staker,
	 operator,
	 block_time,
	 date_trunc('day', block_time) + INTERVAL '1' day AS snapshot_time
 from ranked_delegations
 where rn = 1
),
-- Get the range for each staker
staker_delegation_windows as (
 SELECT
	 staker, operator, snapshot_time as start_time,
	 CASE
		 -- If the range does not have the end, use the cutoff date truncated to 0 UTC
		 WHEN LEAD(snapshot_time) OVER (PARTITION BY staker ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{.cutoffDate}}')
		 ELSE LEAD(snapshot_time) OVER (PARTITION BY staker ORDER BY snapshot_time)
		 END AS end_time
 FROM snapshotted_records
),
cleaned_records as (
	SELECT * FROM staker_delegation_windows
	WHERE start_time < end_time
),
final_results as (
	SELECT
		staker,
		operator,
		d AS snapshot
	FROM
		cleaned_records
			CROSS JOIN
		generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS d
)
select
    staker,
    operator,
    snapshot
from final_results
on conflict on constraint uniq_staker_delegation_snapshots do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertStakerDelegationSnapshots(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(stakerDelegationSnapshotsQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render operator share snapshots query", "error", err)
		return nil
	}

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate staker_delegation_snapshots",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) ListStakerDelegationSnapshots() ([]*StakerDelegationSnapshot, error) {
	var stakerDelegationSnapshots []*StakerDelegationSnapshot
	res := rc.grm.Model(&StakerDelegationSnapshot{}).Find(&stakerDelegationSnapshots)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list staker delegation snapshots", "error", res.Error)
		return nil, res.Error
	}
	return stakerDelegationSnapshots, nil
}
