package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

// @information Edge Cases
/**
 * 1. Max Magnitude can be 0 OR null. If null, then default to 1e18. If 0, then use 0
 * 2. Allocation can be 0 but NOT null. If 0, then there is no slashable stake for that operator/operatorSet
 * 3. Operators can allocate without being registered.
 * 4. Operators can be allocated to non-registered strategies (eg. strategy removed from operator set)
 * 5. Operator can deallocate then have a strategy removed from the operatorSet
 *    NOTE: in 4/5, the operator should stop earning unique stake rewards immediately
 * 6. Scenario -> AVS scams operators. This is a known edge case -> We should document it. The AVS MUST be honest.
      - Day 1: Operator allocates to Strategy A for 100 ETH
	  - Day 2: Strategy removed from operatorSet. Operator doesn't earn rewards
	  - Day 3: Strategy added back to operatorSet. Operator earns rewards again
*/

// Scenario: Deallocation & then slash.
// Day 1: Bob allocates 100% of his stake to Strategy A on 1/2
// Day 2: Bob deallocates to 50% of his stake. Block: 100. Effective Date: 1/16. Effective Block: 200
// Day 3: Bob is slashed 60%% of his stake.
//   - 3 Events
//   - 1. Allocation is updated from 100% to 40% immediately.
//   - 2. Max Magnitude is updated from 100% to 40% immediately.
//   - 3. Allocation (for pending deallocation) is updated from 50% to 20%.
//     -> Block: 110. Effective Date: 1/16. Effective Block: 200

// @audit In above scenario, we handle properly as long as we do not default to the block number if the effective_block is not available.
const operatorAllocationSnapshotsQuery = `
	insert into operator_allocation_snapshots(operator, avs, strategy, operator_set_id, magnitude, max_magnitude, snapshot)
	WITH ranked_allocation_records as (
		SELECT *,
		       -- @audit: We should only order by block_time DESC (other tables do this). Ordering by both block_time and block_number provide no additional info on ordering. 
			   ROW_NUMBER() OVER (PARTITION BY operator, avs, strategy, operator_set_id, cast(block_time AS DATE) ORDER BY block_time DESC, oa.block_number DESC, log_index DESC) AS rn
		FROM operator_allocations oa
		-- Backward compatibility: use effective_block if available, fall back to block_number for old records
		-- @audit: We should not default to the block number if the effective_block is not available.
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
			-- @audit information: previous_magnitude not used anywhere. Probably make sense to set this value in a previous CTE and then use it here for readability
			LAG(magnitude) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY block_time, block_number, log_index) as previous_magnitude,
			-- Allocation (increase) rounds UP, deallocation (decrease) rounds DOWN
			CASE
				WHEN LAG(magnitude) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY block_time, block_number, log_index) IS NULL THEN
					-- First allocation: round up to next day
					date_trunc('day', block_time) + INTERVAL '1' day
				WHEN magnitude > LAG(magnitude) OVER (PARTITION BY operator, avs, strategy, operator_set_id ORDER BY block_time, block_number, log_index) THEN
					-- Increase: round up to next day
					date_trunc('day', block_time) + INTERVAL '1' day
				ELSE -- @information. No change is not possible, so it is less than. 
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
		FROM cleaned_records
		CROSS JOIN generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
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
		FROM max_magnitude_windows
		CROSS JOIN generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
		WHERE start_time < end_time
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
	// TODO: let's look into this edge case.
	// Scenario: Same day
	// 1. Bob allocated 100% of his stake to Strategy A.
	// 2. Bob is slashed 20%, his allocation is now 80%. MaxMagnitude is 80%
	// 3. Bob deallocates all of his stake from Strategy A.
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
