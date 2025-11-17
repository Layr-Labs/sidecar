package rewards

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

// deallocationQueueShareSnapshotsQuery calculates operator allocations that should
// still be considered for rewards because the effective_date hasn't been reached yet.
//
// Logic (similar to withdrawals but for operator allocations):
// - When an operator deallocates (reduces allocation), effective_date is set (rounded DOWN)
// - The allocation magnitude is immediately updated in operator_allocations table
// - But for rewards, the OLD (higher) allocation should be used until effective_date
// - After effective_date, use the NEW (lower) allocation
//
// For a given snapshot_date, we need to identify deallocations where:
// - block_date <= snapshot_date (deallocation already recorded)
// - effective_date > snapshot_date (not yet effective)
//
// This is conceptually similar to withdrawal queue but uses effective_date instead of
// a fixed 14-day period.
//
// NOTE: This handles DECREASES (deallocations). Increases (allocations) are simpler
// because they round UP, so they naturally start earning on effective_date.
const deallocationQueueShareSnapshotsQuery = `
	with deallocation_adjustments as (
		select
			oa.operator,
			oa.avs,
			oa.strategy,
			oa.magnitude as new_magnitude,
			oa.effective_date,
			date(b.block_time) as block_date,
			oa.block_number,
			-- Get previous allocation to calculate the difference
			lag(oa.magnitude) over (
				partition by oa.operator, oa.avs, oa.strategy
				order by oa.block_number, oa.log_index
			) as prev_magnitude
		from operator_allocations oa
		inner join blocks b on oa.block_number = b.number
		where
			oa.effective_date is not null  -- Only Sabine fork records
			-- Allocation already recorded before or on snapshot
			and date(b.block_time) <= '{{.snapshotDate}}'
			-- Effective date is in the future relative to snapshot
			and oa.effective_date > '{{.snapshotDate}}'
	),
	deallocations_only as (
		select
			operator,
			avs,
			strategy,
			new_magnitude,
			prev_magnitude,
			(prev_magnitude::numeric - new_magnitude::numeric) as magnitude_decrease,
			effective_date,
			block_date
		from deallocation_adjustments
		where
			-- Only deallocations (decreases)
			prev_magnitude is not null
			and new_magnitude::numeric < prev_magnitude::numeric
	)
	insert into deallocation_queue_snapshots(
		operator,
		avs,
		strategy,
		magnitude_decrease,
		block_date,
		effective_date,
		snapshot
	)
	select
		operator,
		avs,
		strategy,
		magnitude_decrease,
		block_date,
		effective_date,
		'{{.snapshotDate}}'::date as snapshot
	from deallocations_only
	on conflict on constraint uniq_deallocation_queue_snapshots do nothing;
`

// DeallocationQueueSnapshot represents operator allocation decreases that haven't
// reached their effective_date yet, and should still be counted for rewards.
type DeallocationQueueSnapshot struct {
	Operator          string `gorm:"column:operator;primaryKey"`
	Avs               string `gorm:"column:avs;primaryKey"`
	Strategy          string `gorm:"column:strategy;primaryKey"`
	MagnitudeDecrease string `gorm:"column:magnitude_decrease"`
	BlockDate         string `gorm:"column:block_date"`
	EffectiveDate     string `gorm:"column:effective_date"`
	Snapshot          string `gorm:"column:snapshot;primaryKey"`
}

func (DeallocationQueueSnapshot) TableName() string {
	return "deallocation_queue_snapshots"
}

// GenerateAndInsertDeallocationQueueSnapshots calculates and inserts operator allocation
// decreases that should still be counted for rewards because their effective_date hasn't
// been reached yet.
//
// This feature is only active after the Sabine fork (when effective_date is populated).
func (r *RewardsCalculator) GenerateAndInsertDeallocationQueueSnapshots(snapshotDate string) error {
	// Check if we're past the Sabine fork
	forks, err := r.globalConfig.GetRewardsSqlForkDates()
	if err != nil {
		r.logger.Sugar().Errorw("Failed to get rewards fork dates", "error", err)
		return err
	}

	sabineFork, exists := forks[config.RewardsFork_Sabine]
	if !exists {
		r.logger.Sugar().Warnw("Sabine fork not configured, skipping deallocation queue logic")
		return nil
	}

	// Get the block number for the snapshot date to compare with fork block
	var maxBlock uint64
	res := r.grm.Raw(`
		SELECT COALESCE(MAX(number), 0) as max_block
		FROM blocks
		WHERE DATE(block_time) <= ?
	`, snapshotDate).Scan(&maxBlock)

	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to get max block for snapshot date", "error", res.Error)
		return res.Error
	}

	// Only apply deallocation queue logic if we're past the Sabine fork
	if maxBlock < sabineFork.BlockNumber {
		r.logger.Sugar().Debugw("Snapshot date is before Sabine fork, skipping deallocation queue logic",
			zap.String("snapshotDate", snapshotDate),
			zap.Uint64("maxBlock", maxBlock),
			zap.Uint64("sabineForkBlock", sabineFork.BlockNumber),
		)
		return nil
	}

	query, err := rewardsUtils.RenderQueryTemplate(deallocationQueueShareSnapshotsQuery, map[string]interface{}{
		"snapshotDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render deallocation queue snapshots query template", "error", err)
		return err
	}

	res = r.grm.Debug().Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to insert deallocation_queue_snapshots",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
	}

	r.logger.Sugar().Infow("Generated deallocation queue snapshots",
		zap.String("snapshotDate", snapshotDate),
		zap.Int64("rowsAffected", res.RowsAffected),
	)

	return nil
}

// ListDeallocationQueueSnapshots returns all deallocation queue snapshots for debugging
func (r *RewardsCalculator) ListDeallocationQueueSnapshots() ([]*DeallocationQueueSnapshot, error) {
	var snapshots []*DeallocationQueueSnapshot
	res := r.grm.Model(&DeallocationQueueSnapshot{}).Find(&snapshots)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to list deallocation queue snapshots", "error", res.Error)
		return nil, res.Error
	}
	return snapshots, nil
}

// adjustOperatorAllocationSnapshotsForDeallocationQueueQuery adds deallocation queue
// allocations back to operator allocation snapshots.
//
// This is similar to withdrawal queue adjustment but for operator allocations.
// We add back the magnitude_decrease to the current allocation to get the pre-deallocation value.
const adjustOperatorAllocationSnapshotsForDeallocationQueueQuery = `
	-- NOT YET IMPLEMENTED
	-- This would need to integrate with operator allocation snapshots used in rewards v2.2
	-- For now, this is a placeholder for future operator set rewards calculation
	-- TODO: Integrate with operator set rewards calculation
`

// AdjustOperatorAllocationSnapshotsForDeallocationQueue adds deallocation queue
// allocations back to operator allocation snapshots.
//
// NOTE: This is a placeholder for integration with operator set rewards (v2.2).
// The actual adjustment logic will depend on how operator allocations are used in rewards.
func (r *RewardsCalculator) AdjustOperatorAllocationSnapshotsForDeallocationQueue(snapshotDate string) error {
	// TODO: Implement adjustment logic once operator set rewards calculation structure is finalized
	r.logger.Sugar().Debugw("Deallocation queue adjustment not yet implemented",
		zap.String("snapshotDate", snapshotDate),
	)
	return nil
}
