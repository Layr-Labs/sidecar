package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const operatorShareSnapshotsQuery = `
insert into operator_share_snapshots (operator, strategy, shares, snapshot)
WITH ranked_operator_records as (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY operator, strategy, cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
    FROM operator_shares
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
),
base_snapshots as (
	SELECT
		operator,
		strategy,
		shares,
		cast(day AS DATE) AS snapshot
	FROM
		cleaned_records
	CROSS JOIN
		generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
),
-- Add operator allocations (deallocation delay handled via effective_block)
allocation_adjustments as (
	SELECT
		oas.operator,
		oas.strategy,
		SUM(oas.magnitude) as total_magnitude,
		oas.snapshot
	FROM operator_allocation_snapshots oas
	WHERE oas.snapshot = DATE '{{.snapshotDate}}'
	GROUP BY oas.operator, oas.strategy, oas.snapshot
),
combined_snapshots as (
	SELECT
		coalesce(base.operator, alloc.operator) as operator,
		coalesce(base.strategy, alloc.strategy) as strategy,
		coalesce(alloc.total_magnitude, base.shares::numeric) as shares,
		coalesce(base.snapshot, alloc.snapshot) as snapshot
	FROM base_snapshots base
	FULL OUTER JOIN allocation_adjustments alloc
		ON base.operator = alloc.operator
		AND base.strategy = alloc.strategy
		AND base.snapshot = alloc.snapshot
)
SELECT * FROM combined_snapshots
on conflict on constraint uniq_operator_share_snapshots do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertOperatorShareSnapshots(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(operatorShareSnapshotsQuery, map[string]interface{}{
		"cutoffDate":   snapshotDate,
		"snapshotDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render operator share snapshots query", "error", err)
		return err
	}

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate operator_share_snapshots",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
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
