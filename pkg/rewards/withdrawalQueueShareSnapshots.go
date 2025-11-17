package rewards

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

// withdrawalQueueSharesQuery calculates shares that should still earn rewards
// because they are in the withdrawal queue (taking slashing risk).
//
// Logic:
// - Withdrawals are queued when SlashingWithdrawalQueued event occurs
// - Shares become withdrawable after 14 days (withdrawable_date)
// - Until withdrawable_date, shares should still earn rewards
// - After withdrawable_date, staker can withdraw at any time (no longer forced risk)
//
// For a given snapshot_date, we add back shares where:
// - queued_date <= snapshot_date (withdrawal was already queued)
// - withdrawable_date > snapshot_date (shares still in forced queue period)
//
// NOTE: We do NOT check 'completed = false' because:
// 1. Withdrawal can be completed anytime after withdrawable_date
// 2. Once withdrawable_date passes, staker is no longer forced to take risk
// 3. So rewards should stop at withdrawable_date, not completion_date
//
// The query joins with blocks table to derive timestamps from block_number.
const withdrawalQueueShareSnapshotsQuery = `
	with withdrawal_queue_adjustments as (
		select
			qsw.staker,
			qsw.strategy,
			qsw.shares_to_withdraw as shares,
			b_queued.block_time as queued_timestamp,
			date(b_queued.block_time) as queued_date,
			date(b_queued.block_time) + interval '14 days' as withdrawable_date,
			qsw.block_number as queued_block_number
		from queued_slashing_withdrawals qsw
		inner join blocks b_queued on qsw.block_number = b_queued.number
		where
			date(b_queued.block_time) <= '{{.snapshotDate}}'
			and date(b_queued.block_time) + interval '14 days' > '{{.snapshotDate}}'
	)
	insert into withdrawal_queue_share_snapshots(
		staker,
		strategy,
		shares,
		queued_date,
		withdrawable_date,
		snapshot
	)
	select
		staker,
		strategy,
		shares,
		queued_date,
		withdrawable_date,
		'{{.snapshotDate}}'::date as snapshot
	from withdrawal_queue_adjustments
	on conflict on constraint uniq_withdrawal_queue_share_snapshots do nothing;
`

type WithdrawalQueueShareSnapshot struct {
	Staker           string `gorm:"column:staker;primaryKey"`
	Strategy         string `gorm:"column:strategy;primaryKey"`
	Shares           string `gorm:"column:shares"`
	QueuedDate       string `gorm:"column:queued_date"`
	WithdrawableDate string `gorm:"column:withdrawable_date"`
	Snapshot         string `gorm:"column:snapshot;primaryKey"`
}

func (WithdrawalQueueShareSnapshot) TableName() string {
	return "withdrawal_queue_share_snapshots"
}

// GenerateAndInsertWithdrawalQueueShares calculates and inserts shares in withdrawal queue
// that should still earn rewards for the given snapshot date.
//
// This feature is only active after the Sabine fork.
func (r *RewardsCalculator) GenerateAndInsertWithdrawalQueueShares(snapshotDate string) error {
	forks, err := r.globalConfig.GetRewardsSqlForkDates()
	if err != nil {
		r.logger.Sugar().Errorw("Failed to get rewards fork dates", "error", err)
		return err
	}

	sabineFork, exists := forks[config.RewardsFork_Sabine]
	if !exists {
		r.logger.Sugar().Warnw("Sabine fork not configured, skipping withdrawal queue logic")
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

	if maxBlock < sabineFork.BlockNumber {
		r.logger.Sugar().Debugw("Snapshot date is before Sabine fork, skipping withdrawal queue logic",
			zap.String("snapshotDate", snapshotDate),
			zap.Uint64("maxBlock", maxBlock),
			zap.Uint64("sabineForkBlock", sabineFork.BlockNumber),
		)
		return nil
	}

	query, err := rewardsUtils.RenderQueryTemplate(withdrawalQueueShareSnapshotsQuery, map[string]interface{}{
		"snapshotDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render withdrawal queue share snapshots query template", "error", err)
		return err
	}

	res = r.grm.Debug().Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to insert withdrawal_queue_share_snapshots",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
	}

	r.logger.Sugar().Infow("Generated withdrawal queue share snapshots",
		zap.String("snapshotDate", snapshotDate),
		zap.Int64("rowsAffected", res.RowsAffected),
	)

	return nil
}

// ListWithdrawalQueueShareSnapshots returns all withdrawal queue share snapshots for debugging
func (r *RewardsCalculator) ListWithdrawalQueueShareSnapshots() ([]*WithdrawalQueueShareSnapshot, error) {
	var snapshots []*WithdrawalQueueShareSnapshot
	res := r.grm.Model(&WithdrawalQueueShareSnapshot{}).Find(&snapshots)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to list withdrawal queue share snapshots", "error", res.Error)
		return nil, res.Error
	}
	return snapshots, nil
}

// adjustStakerShareSnapshotsForWithdrawalQueueQuery adds withdrawal queue shares
// to existing staker_share_snapshots for the given snapshot date.
//
// This is executed AFTER GenerateAndInsertStakerShareSnapshots to add back shares
// that are in the withdrawal queue and should still earn rewards.
const adjustStakerShareSnapshotsForWithdrawalQueueQuery = `
	-- Add withdrawal queue shares to existing staker share snapshots
	-- If a staker/strategy already exists, add to their shares
	-- Otherwise, insert a new row
	insert into staker_share_snapshots(staker, strategy, shares, snapshot)
	select
		wqss.staker,
		wqss.strategy,
		wqss.shares,
		wqss.snapshot as snapshot
	from withdrawal_queue_share_snapshots wqss
	where wqss.snapshot = '{{.snapshotDate}}'
	on conflict on constraint uniq_staker_share_snapshots
	do update set
		-- Add withdrawal queue shares to existing snapshot shares
		shares = staker_share_snapshots.shares + EXCLUDED.shares;
`

// AdjustStakerShareSnapshotsForWithdrawalQueue adds withdrawal queue shares to
// staker share snapshots. This ensures stakers continue earning rewards while
// their withdrawals are in the 14-day queue.
func (r *RewardsCalculator) AdjustStakerShareSnapshotsForWithdrawalQueue(snapshotDate string) error {
	query, err := rewardsUtils.RenderQueryTemplate(adjustStakerShareSnapshotsForWithdrawalQueueQuery, map[string]interface{}{
		"snapshotDate": snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render withdrawal queue adjustment query template", "error", err)
		return err
	}

	res := r.grm.Debug().Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to adjust staker_share_snapshots for withdrawal queue",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
	}

	r.logger.Sugar().Infow("Adjusted staker share snapshots for withdrawal queue",
		zap.String("snapshotDate", snapshotDate),
		zap.Int64("rowsAffected", res.RowsAffected),
	)

	return nil
}
