package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const operatorPISplitSnapshotQuery = `
insert into operator_pi_split_snapshots (operator, split, snapshot)
WITH operator_pi_splits_with_block_info as (
	select
		ops.operator,
		ops.activated_at::timestamp(6) as activated_at,
		ops.new_operator_pi_split_bips as split,
		ops.block_number,
		ops.log_index,
		b.block_time::timestamp(6) as block_time
	from operator_pi_splits as ops
	join blocks as b on (b.number = ops.block_number)
	where activated_at < TIMESTAMP '{{.cutoffDate}}'
),
-- Rank the records for each operator by block time asc, log index asc (in the order they happened)
ranked_operator_pi_split_records as (
	SELECT
	    *,
		ROW_NUMBER() OVER (PARTITION BY operator, cast(activated_at AS DATE) ORDER BY activated_at DESC, block_time DESC, log_index DESC) AS rn
	FROM operator_pi_splits_with_block_info
),
-- Get the latest record for each day & round up to the snapshot day
snapshotted_records as (
 SELECT
	 operator,
	 split,
	 block_time,
	 date_trunc('day', activated_at) + INTERVAL '1' day AS snapshot_time
 from ranked_operator_pi_split_records
 where rn = 1
),
-- Get the range for each operator
operator_pi_split_windows as (
 SELECT
	 operator,
	 split,
	 snapshot_time as start_time,
	 CASE
		 -- If the range does not have the end, use the current timestamp truncated to 0 UTC
		 WHEN LEAD(snapshot_time) OVER (PARTITION BY operator ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{.cutoffDate}}')
		 ELSE LEAD(snapshot_time) OVER (PARTITION BY operator ORDER BY snapshot_time)
		 END AS end_time
 FROM snapshotted_records
),
-- Clean up any records where start_time >= end_time
cleaned_records as (
  SELECT * FROM operator_pi_split_windows
  WHERE start_time < end_time
),
-- Generate a snapshot for each day in the range
final_results as (
	SELECT
		operator,
		split,
		d AS snapshot
	FROM
		cleaned_records
			CROSS JOIN
		generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS d
)
select
	operator,
	split,
	snapshot
from final_results
on conflict on constraint uniq_operator_pi_split_snapshots do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertOperatorPISplitSnapshots(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(operatorPISplitSnapshotQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", zap.Error(err))
		return err
	}

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate operator_pi_split_snapshots",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
	}
	return nil
}

func (r *RewardsCalculator) ListOperatorPISplitSnapshots() ([]*OperatorPISplitSnapshots, error) {
	var snapshots []*OperatorPISplitSnapshots
	res := r.grm.Model(&OperatorPISplitSnapshots{}).Find(&snapshots)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to list operator pi split snapshots", "error", res.Error)
		return nil, res.Error
	}
	return snapshots, nil
}
