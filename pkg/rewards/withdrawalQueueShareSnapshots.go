package rewards

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

// Shares in withdrawal queue still earn rewards during 14-day period (stakers taking slashing risk)
// Rewards stop at withdrawable_date, not completion_date
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

// NOTE: Adjustments are materialized into staker_share_snapshots during generation
// The withdrawal_queue_share_snapshots table serves as an audit trail and source data
