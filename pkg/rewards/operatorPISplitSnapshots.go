package rewards

import "github.com/Layr-Labs/sidecar/pkg/rewardsUtils"

const operatorPISplitSnapshotQuery = `
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
	    -- round activated up to the nearest day
	    date_trunc('day', activated_at) + INTERVAL '1' day AS rounded_activated_at,
		ROW_NUMBER() OVER (PARTITION BY operator ORDER BY block_time asc, log_index asc) AS rn
	FROM operator_pi_splits_with_block_info
),
decorated_operator_splits as (
    select
        rops.*,
        -- if there is a row, we have found another split that overlaps the current split
        -- meaning the current split should be discarded
        case when rops2.block_time is not null then false else true end as active
    from ranked_operator_pi_split_records as rops
    left join ranked_operator_pi_split_records as rops2 on (
        rops.operator = rops2.operator
        -- rn is orderd by block and log_index, so this should encapsulate rops2 occurring afer rops
        and rops.rn > rops2.rn
        -- only find the next split that overlaps with the current one
        and rops2.rounded_activated_at <= rops.rounded_activated_at
    )
),
-- filter in only splits flagged as active
active_operator_splits as (
    select
        *,
        rounded_activated_at as snapshot_time,
        ROW_NUMBER() over (partition by operator order by rounded_activated_at asc) as rn
    from decorated_operator_splits
    where active = true
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

		 -- need to subtract 1 day from the end time since generate_series will be inclusive below.
		 ELSE LEAD(snapshot_time) OVER (PARTITION BY operator ORDER BY snapshot_time) - interval '1 day'
		 END AS end_time
 FROM active_operator_splits
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
select * from final_results
`

func (r *RewardsCalculator) GenerateAndInsertOperatorPISplitSnapshots(snapshotDate string) error {
	tableName := "operator_pi_split_snapshots"

	query, err := rewardsUtils.RenderQueryTemplate(operatorPISplitSnapshotQuery, map[string]string{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	err = r.generateAndInsertFromQuery(tableName, query, nil)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate operator_pi_split_snapshots", "error", err)
		return err
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
