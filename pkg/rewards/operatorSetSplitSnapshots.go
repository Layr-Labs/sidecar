package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const operatorSetSplitSnapshotQuery = `
insert into operator_set_split_snapshots (operator, avs, operator_set_id, split, snapshot)
WITH operator_set_splits_with_block_info as (
	select
		oss.operator,
		oss.avs,
		oss.operator_set_id,
		oss.activated_at::timestamp(6) as activated_at,
		LEAST(oss.new_operator_set_split_bips, 10000) as split,
		oss.block_number,
		oss.log_index,
		b.block_time::timestamp(6) as block_time
	from operator_set_splits as oss
	join blocks as b on (b.number = oss.block_number)
	where activated_at < TIMESTAMP '{{.cutoffDate}}'
),
-- Rank the records for each combination of (operator, avs, operator_set_id, activation date) by activation time, block time and log index
ranked_operator_set_split_records as (
	SELECT
	    *,
		ROW_NUMBER() OVER (PARTITION BY operator, avs, operator_set_id, cast(activated_at AS DATE) ORDER BY activated_at DESC, block_time DESC, log_index DESC) AS rn
	FROM operator_set_splits_with_block_info
),
-- Get the latest record for each day & round up to the snapshot day
snapshotted_records as (
 SELECT
	 operator,
	 avs,
	 operator_set_id,
	 split,
	 block_time,
	 date_trunc('day', activated_at) + INTERVAL '1' day AS snapshot_time
 from ranked_operator_set_split_records
 where rn = 1
),
-- Get the range for each (operator, avs, operator_set_id) grouping
operator_set_split_windows as (
 SELECT
	 operator, avs, operator_set_id, split, snapshot_time as start_time,
	 CASE
		 -- If the range does not have the end, use the current timestamp truncated to 0 UTC
		 WHEN LEAD(snapshot_time) OVER (PARTITION BY operator, avs, operator_set_id ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{.cutoffDate}}')
		 ELSE LEAD(snapshot_time) OVER (PARTITION BY operator, avs, operator_set_id ORDER BY snapshot_time)
		 END AS end_time
 FROM snapshotted_records
),
-- Clean up any records where start_time >= end_time
cleaned_records as (
  SELECT * FROM operator_set_split_windows
  WHERE start_time < end_time
),
-- Generate a snapshot for each day in the range
final_results as (
	SELECT
		operator,
		avs,
		operator_set_id,
		split,
		d AS snapshot
	FROM
		cleaned_records
			CROSS JOIN
		generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS d
)
select * from final_results
on conflict on constraint uniq_operator_set_split_snapshots do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertOperatorSetSplitSnapshots(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(operatorSetSplitSnapshotQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate operator_set_split_snapshots",
			zap.Error(res.Error),
			zap.String("snapshotDate", snapshotDate),
		)
		return res.Error
	}
	return nil
}

func (r *RewardsCalculator) ListOperatorSetSplitSnapshots() ([]*OperatorSetSplitSnapshots, error) {
	var snapshots []*OperatorSetSplitSnapshots
	res := r.grm.Model(&OperatorSetSplitSnapshots{}).Find(&snapshots)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to list operator set split snapshots", "error", res.Error)
		return nil, res.Error
	}
	return snapshots, nil
}
