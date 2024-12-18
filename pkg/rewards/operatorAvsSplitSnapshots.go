package rewards

import "github.com/Layr-Labs/sidecar/pkg/rewardsUtils"

const operatorAvsSplitSnapshotQuery = `
WITH operator_avs_splits_with_block_info as (
	select
		oas.operator,
		oas.avs,
		oas.activated_at::timestamp(6) as activated_at,
		oas.new_operator_avs_split_bips as split,
		oas.block_number,
		oas.log_index,
		b.block_time::timestamp(6) as block_time
	from operator_avs_splits as oas
	join blocks as b on (b.number = oas.block_number)
	where activated_at < TIMESTAMP '{{.cutoffDate}}'
),
-- Rank the records for each combination of (operator, avs, activation date) by activation time, block time and log index
ranked_operator_avs_split_records as (
	SELECT
	    *,
		ROW_NUMBER() OVER (PARTITION BY operator, avs, cast(activated_at AS DATE) ORDER BY activated_at DESC, block_time DESC, log_index DESC) AS rn
	FROM operator_avs_splits_with_block_info
),
-- Get the latest record for each day & round up to the snapshot day
snapshotted_records as (
 SELECT
	 operator,
	 avs,
	 split,
	 block_time,
	 date_trunc('day', activated_at) + INTERVAL '1' day AS snapshot_time
 from ranked_operator_avs_split_records
 where rn = 1
),
-- Get the range for each operator, avs pairing
operator_avs_split_windows as (
 SELECT
	 operator, avs, split, snapshot_time as start_time,
	 CASE
		 -- If the range does not have the end, use the current timestamp truncated to 0 UTC
		 WHEN LEAD(snapshot_time) OVER (PARTITION BY operator, avs ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{.cutoffDate}}')
		 ELSE LEAD(snapshot_time) OVER (PARTITION BY operator, avs ORDER BY snapshot_time)
		 END AS end_time
 FROM snapshotted_records
),
-- Clean up any records where start_time >= end_time
cleaned_records as (
  SELECT * FROM operator_avs_split_windows
  WHERE start_time < end_time
),
-- Generate a snapshot for each day in the range
final_results as (
	SELECT
		operator,
		avs,
		split,
		d AS snapshot
	FROM
		cleaned_records
			CROSS JOIN
		generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS d
)
select * from final_results
`

func (r *RewardsCalculator) GenerateAndInsertOperatorAvsSplitSnapshots(snapshotDate string) error {
	tableName := "operator_avs_split_snapshots"

	query, err := rewardsUtils.RenderQueryTemplate(operatorAvsSplitSnapshotQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	err = r.generateAndInsertFromQuery(tableName, query, nil)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate operator_avs_split_snapshots", "error", err)
		return err
	}
	return nil
}

func (r *RewardsCalculator) ListOperatorAvsSplitSnapshots() ([]*OperatorAVSSplitSnapshots, error) {
	var snapshots []*OperatorAVSSplitSnapshots
	res := r.grm.Model(&OperatorAVSSplitSnapshots{}).Find(&snapshots)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to list operator avs split snapshots", "error", res.Error)
		return nil, res.Error
	}
	return snapshots, nil
}
