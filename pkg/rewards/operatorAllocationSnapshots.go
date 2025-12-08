package rewards

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const operatorAllocationSnapshotsQuery = `
	insert into operator_allocation_snapshots(operator, avs, strategy, operator_set_id, magnitude, max_magnitude, snapshot)
	WITH ranked_allocation_records as (
		SELECT *,
			   ROW_NUMBER() OVER (PARTITION BY operator, avs, strategy, operator_set_id, cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
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
	-- Compare each record with the previous record to determine if it's an increase or decrease
	records_with_comparison as (
		SELECT
			operator,
			avs,
			strategy,
			operator_set_id,
			magnitude,
			block_time,
			LAG(magnitude) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY block_time, block_number, log_index) as previous_magnitude,
			-- Backward compatibility: Apply new rounding logic only after Sabine fork
			-- Pre-Sabine: Always round down to current day (old behavior)
			-- Post-Sabine: Allocation (increase) rounds UP, deallocation rounds DOWN
			CASE
				{{ if .useSabineRounding }}
				-- Post-Sabine fork rounding logic
				WHEN LAG(magnitude) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY block_time, block_number, log_index) IS NULL THEN
					-- First allocation: round up to next day
					date_trunc('day', block_time) + INTERVAL '1' day
				WHEN magnitude > LAG(magnitude) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY block_time, block_number, log_index) THEN
					-- Increase: round up to next day
					date_trunc('day', block_time) + INTERVAL '1' day
				ELSE
					-- Decrease or no change: round down to current day
					date_trunc('day', block_time)
				{{ else }}
				-- Pre-Sabine fork rounding logic (backward compatibility)
				WHEN LAG(magnitude) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY block_time, block_number, log_index) IS NULL THEN
					-- First allocation: round down to current day (old behavior)
					date_trunc('day', block_time)
				ELSE
					-- All other cases: round down to current day
					date_trunc('day', block_time)
				{{ end }}
			END AS snapshot_time
		FROM daily_records
	),
	-- Get the range for each operator, avs, strategy, operator_set_id combination
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
	-- Get the latest max_magnitude for each (operator, strategy) as of the cutoff date
	latest_max_magnitudes as (
		SELECT
			omm.operator,
			omm.strategy,
			omm.max_magnitude,
			ROW_NUMBER() OVER (
				PARTITION BY omm.operator, omm.strategy
				ORDER BY omm.block_number DESC, omm.log_index DESC
			) AS rn
		FROM operator_max_magnitudes omm
		INNER JOIN blocks b ON omm.block_number = b.number
		WHERE b.block_time < TIMESTAMP '{{.cutoffDate}}'
	),
	-- Join allocations with max_magnitudes
	allocations_with_max as (
		SELECT
			cr.operator,
			cr.avs,
			cr.strategy,
			cr.operator_set_id,
			cr.magnitude,
			COALESCE(lmm.max_magnitude, '0') as max_magnitude,
			cr.start_time,
			cr.end_time
		FROM cleaned_records cr
		LEFT JOIN latest_max_magnitudes lmm
			ON cr.operator = lmm.operator
			AND cr.strategy = lmm.strategy
			AND lmm.rn = 1
	)
	SELECT
		operator,
		avs,
		strategy,
		operator_set_id,
		magnitude,
		max_magnitude,
		cast(day AS DATE) AS snapshot
	FROM
		allocations_with_max
	CROSS JOIN
		generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
	on conflict do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertOperatorAllocationSnapshots(snapshotDate string) error {
	// Determine if we should use Sabine fork rounding logic based on snapshot date
	forks, err := r.globalConfig.GetRewardsSqlForkDates()
	if err != nil {
		r.logger.Sugar().Errorw("Failed to get rewards fork dates", "error", err)
		return err
	}

	useSabineRounding := false
	if sabineFork, exists := forks[config.RewardsFork_Sabine]; exists {
		// Use new rounding logic if snapshot date is on or after Sabine fork
		useSabineRounding = snapshotDate >= sabineFork.Date
	}

	query, err := rewardsUtils.RenderQueryTemplate(operatorAllocationSnapshotsQuery, map[string]interface{}{
		"cutoffDate":        snapshotDate,
		"useSabineRounding": useSabineRounding,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	r.logger.Sugar().Debugw("Generating operator allocation snapshots",
		zap.String("snapshotDate", snapshotDate),
		zap.Bool("useSabineRounding", useSabineRounding),
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
