package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const defaultOperatorSplitSnapshotQuery = `
insert into default_operator_split_snapshots (split, snapshot)
WITH default_operator_splits_with_block_info as (
	select
		dos.new_default_operator_split_bips as split,
		dos.block_number,
		dos.log_index,
		b.block_time::timestamp(6) as block_time
	from default_operator_splits as dos
	join blocks as b on (b.number = dos.block_number)
	where b.block_time < TIMESTAMP '{{.cutoffDate}}'
),
-- Rank the records for each combination of (block date) by block time and log index
ranked_default_operator_split_records as (
	SELECT
	    *,
		ROW_NUMBER() OVER (PARTITION BY cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
	FROM default_operator_splits_with_block_info
),
-- Get the latest record for each day & round up to the snapshot day
snapshotted_records as (
 SELECT
	 split,
	 block_time,
	 date_trunc('day', block_time) + INTERVAL '1' day AS snapshot_time
 from ranked_default_operator_split_records
 where rn = 1
),
-- Get the range for each operator, avs pairing
default_operator_split_windows as (
 SELECT
	 split, 
	 snapshot_time as start_time,
	 CASE
		 -- If the range does not have the end, use the current timestamp truncated to 0 UTC
		 WHEN LEAD(snapshot_time) OVER (ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{.cutoffDate}}')
		 ELSE LEAD(snapshot_time) OVER (ORDER BY snapshot_time)
		 END AS end_time
 FROM snapshotted_records
),
-- Clean up any records where start_time >= end_time
cleaned_records as (
  SELECT * FROM default_operator_split_windows
  WHERE start_time < end_time
),
-- Generate a snapshot for each day in the range
final_results as (
	SELECT
		split,
		d AS snapshot
	FROM
		cleaned_records
			CROSS JOIN
		generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS d
)
select
	split,
	snapshot
from final_results
on conflict on constraint uniq_default_operator_split_snapshots do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertDefaultOperatorSplitSnapshots(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(defaultOperatorSplitSnapshotQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", zap.Error(err))
		return err
	}

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate default_operator_split_snapshots",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
	}
	return nil
}

func (r *RewardsCalculator) ListDefaultOperatorSplitSnapshots() ([]*DefaultOperatorSplitSnapshots, error) {
	var snapshots []*DefaultOperatorSplitSnapshots
	res := r.grm.Model(&DefaultOperatorSplitSnapshots{}).Find(&snapshots)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to list default operator split snapshots", "error", res.Error)
		return nil, res.Error
	}
	return snapshots, nil
}
