package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const operatorAllocationSnapshotsQuery = `
	insert into operator_allocation_snapshots(operator, avs, strategy, operator_set_id, magnitude, max_magnitude, snapshot)
	WITH ranked_allocation_records as (
		SELECT *,
			   ROW_NUMBER() OVER (PARTITION BY operator, avs, strategy, operator_set_id, cast(block_time AS DATE) ORDER BY block_time DESC, oa.block_number DESC, log_index DESC) AS rn
		FROM operator_allocations oa
		-- Backward compatibility: use effective_block if available, fall back to block_number for old records
		INNER JOIN blocks b ON COALESCE(oa.effective_block, oa.block_number) = b.number
		WHERE b.block_time < TIMESTAMP '{{.cutoffDate}}'
	),
	-- Get the latest record for each day
	daily_records as (
		SELECT
			operator,
			avs,
			strategy,
			operator_set_id,
			magnitude,
			block_time,
			block_number,
			log_index
		FROM ranked_allocation_records
		WHERE rn = 1
	),
	ranked_max_magnitude_records as (
		SELECT
			omm.operator,
			omm.strategy,
			omm.max_magnitude,
			b.block_time,
			omm.block_number,
			omm.log_index,
			ROW_NUMBER() OVER (PARTITION BY omm.operator, omm.strategy, cast(b.block_time AS DATE) ORDER BY b.block_time DESC, omm.log_index DESC) AS rn
		FROM operator_max_magnitudes omm
		INNER JOIN blocks b ON omm.block_number = b.number
		WHERE b.block_time < TIMESTAMP '{{.cutoffDate}}'
	),
	-- Get the latest max_magnitude record for each day
	daily_max_magnitude_records as (
		SELECT
			operator,
			strategy,
			max_magnitude,
			block_time,
			block_number,
			log_index
		FROM ranked_max_magnitude_records
		WHERE rn = 1
	),
	records_with_comparison as (
		SELECT
			operator,
			avs,
			strategy,
			operator_set_id,
			magnitude,
			block_time,
			LAG(magnitude) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY block_time, block_number, log_index) as previous_magnitude,
			-- Allocation (increase) rounds UP, deallocation (decrease) rounds DOWN
			CASE
				WHEN LAG(magnitude) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY block_time, block_number, log_index) IS NULL THEN
					-- First allocation: round up to next day
					date_trunc('day', block_time) + INTERVAL '1' day
				WHEN magnitude > LAG(magnitude) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY block_time, block_number, log_index) THEN
					-- Increase: round up to next day
					date_trunc('day', block_time) + INTERVAL '1' day
				ELSE
					-- Decrease or no change: round down to current day
					date_trunc('day', block_time)
			END AS snapshot_time
		FROM daily_records
	),
	allocation_windows as (
		SELECT
			operator,
			avs,
			strategy,
			operator_set_id,
			magnitude,
			snapshot_time as start_time,
			CASE
				-- If the range does not have the end, use the current timestamp truncated to 0 UTC
				WHEN LEAD(snapshot_time) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{.cutoffDate}}')
				ELSE LEAD(snapshot_time) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY snapshot_time)
			END AS end_time
		FROM records_with_comparison
	),
	cleaned_records as (
		SELECT * FROM allocation_windows
		WHERE start_time < end_time
	),
	daily_allocation_snapshots as (
		SELECT
			operator,
			avs,
			strategy,
			operator_set_id,
			magnitude,
			cast(day AS DATE) AS snapshot
		FROM
			cleaned_records
		CROSS JOIN
			generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
	),
	max_magnitude_windows as (
		SELECT
			operator,
			strategy,
			max_magnitude,
			block_time as start_time,
			CASE
				WHEN LEAD(block_time) OVER (PARTITION BY operator, strategy ORDER BY block_time) is null THEN TIMESTAMP '{{.cutoffDate}}'
				ELSE LEAD(block_time) OVER (PARTITION BY operator, strategy ORDER BY block_time)
			END AS end_time
		FROM daily_max_magnitude_records
	),
	daily_max_magnitude_snapshots as (
		SELECT
			operator,
			strategy,
			max_magnitude,
			cast(day AS DATE) AS snapshot
		FROM
			max_magnitude_windows
		WHERE start_time < end_time
		CROSS JOIN
			generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
	)
	SELECT
		das.operator,
		das.avs,
		das.strategy,
		das.operator_set_id,
		das.magnitude,
		-- Default to 1e18 (contract initialization value) when no MagnitudeUpdated event exists
		COALESCE(dmms.max_magnitude, '1000000000000000000') as max_magnitude,
		das.snapshot
	FROM
		daily_allocation_snapshots das
	LEFT JOIN daily_max_magnitude_snapshots dmms
		ON das.operator = dmms.operator
		AND das.strategy = dmms.strategy
		AND das.snapshot = dmms.snapshot
	on conflict do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertOperatorAllocationSnapshots(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(operatorAllocationSnapshotsQuery, map[string]interface{}{
		"cutoffDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	r.logger.Sugar().Debugw("Generating operator allocation snapshots",
		zap.String("snapshotDate", snapshotDate),
	)

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate operator_allocation_snapshots",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
	}
	return nil
}

func (r *RewardsCalculator) ListOperatorAllocationSnapshots() ([]*OperatorAllocationSnapshot, error) {
	var snapshots []*OperatorAllocationSnapshot
	res := r.grm.Model(&OperatorAllocationSnapshot{}).Find(&snapshots)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to list operator allocation snapshots", "error", res.Error)
		return nil, res.Error
	}
	return snapshots, nil
}
