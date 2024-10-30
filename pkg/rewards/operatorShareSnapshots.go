package rewards

const operatorShareSnapshotsQuery = `
WITH ranked_operator_records as (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY operator, strategy, cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
    FROM operator_share_deltas
	-- pipeline bronze table uses this to filter the correct records
	where block_time < TIMESTAMP '{{.cutoffDate}}'
),
-- Get the latest record for each day & round up to the snapshot day
snapshotted_records as (
 SELECT
	 operator,
	 strategy,
	 shares,
	 block_time,
	 date_trunc('day', block_time) + INTERVAL '1' day as snapshot_time
 from ranked_operator_records
 where rn = 1
),
-- Get the range for each operator, strategy pairing
operator_share_windows as (
	SELECT
		operator, strategy, shares, snapshot_time as start_time,
		CASE
			-- If the range does not have the end, use the current timestamp truncated to 0 UTC
			WHEN LEAD(snapshot_time) OVER (PARTITION BY operator, strategy ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{.cutoffDate}}')
			ELSE LEAD(snapshot_time) OVER (PARTITION BY operator, strategy ORDER BY snapshot_time)
		END AS end_time
	FROM snapshotted_records
),
cleaned_records as (
	SELECT * FROM operator_share_windows
	WHERE start_time < end_time
)
SELECT
    operator,
    strategy,
    shares,
    cast(day AS DATE) AS snapshot
FROM
    cleaned_records
        CROSS JOIN
    generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
`

func (r *RewardsCalculator) GenerateAndInsertOperatorShareSnapshots(startDate string, snapshotDate string) error {
	tableName := "operator_share_snapshots"

	query, err := renderQueryTemplate(operatorShareSnapshotsQuery, map[string]string{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render operator share snapshots query", "error", err)
		return err
	}

	err = r.generateAndInsertFromQuery(tableName, query, nil)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to generate operator_share_snapshots", "error", err)
		return err
	}
	return nil
}

func (r *RewardsCalculator) ListOperatorShareSnapshots() ([]*OperatorShareSnapshots, error) {
	var operatorShareSnapshots []*OperatorShareSnapshots
	res := r.grm.Model(&OperatorShareSnapshots{}).Find(&operatorShareSnapshots)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to list operator share snapshots", "error", res.Error)
		return nil, res.Error
	}
	return operatorShareSnapshots, nil
}
