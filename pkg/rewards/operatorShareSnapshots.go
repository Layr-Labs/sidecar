package rewards

import (
	"github.com/Layr-Labs/sidecar/internal/config"
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
		operator, strategy, shares::numeric as shares, snapshot_time as start_time,
		CASE
			-- If the range does not have the end, use cutoff + 1 day to include cutoff date snapshot
			WHEN LEAD(snapshot_time) OVER (PARTITION BY operator, strategy ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{.cutoffDate}}') + INTERVAL '1' day
			ELSE LEAD(snapshot_time) OVER (PARTITION BY operator, strategy ORDER BY snapshot_time)
		END AS end_time
	FROM snapshotted_records
),
cleaned_records as (
	SELECT * FROM operator_share_windows
	WHERE start_time < end_time
)
SELECT
	cr.operator,
	cr.strategy,
	{{ if .useSabineFork }}
	-- Post-Sabine fork: Add back queued withdrawal shares during queue period
	-- This ensures operators show the correct slashable stake during the withdrawal queue window
	(cr.shares + COALESCE(
		(SELECT SUM(
			qsw.shares_to_withdraw::numeric * COALESCE(
				-- Get the latest slash multiplier visible BEFORE this snapshot day
				(SELECT adj.slash_multiplier::numeric
				 FROM queued_withdrawal_slashing_adjustments adj
				 INNER JOIN blocks b_slash ON adj.slash_block_number = b_slash.number
				 WHERE adj.operator = qsw.operator
				 AND adj.strategy = qsw.strategy
				 AND adj.withdrawal_block_number = qsw.block_number
				 AND adj.withdrawal_log_index = qsw.log_index
				 AND DATE(b_slash.block_time) < day::date
				 ORDER BY adj.slash_block_number DESC
				 LIMIT 1),
				1  -- No slashing if no adjustment records visible yet
			)
		)
		 FROM queued_slashing_withdrawals qsw
		 INNER JOIN blocks b_queued ON qsw.block_number = b_queued.number
		 WHERE qsw.operator = cr.operator
		 AND qsw.strategy = cr.strategy
		 -- Withdrawal must be queued before the snapshot day
		 AND DATE(b_queued.block_time) < day::date
		 -- Withdrawal must still be in queue (not yet completable) on this snapshot day
		 AND b_queued.block_time + INTERVAL '{{.withdrawalQueueWindow}} days' > day::timestamp
		 -- Backwards compatibility: only process records with valid data
		 AND qsw.staker IS NOT NULL
		 AND qsw.strategy IS NOT NULL
		 AND qsw.operator IS NOT NULL
		 AND qsw.shares_to_withdraw IS NOT NULL
		 AND b_queued.block_time IS NOT NULL
		), 0
	))::numeric as shares,
	{{ else }}
	-- Pre-Sabine fork: Use base shares only (old behavior)
	cr.shares as shares,
	{{ end }}
	cast(day AS DATE) AS snapshot
FROM cleaned_records cr
CROSS JOIN generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
on conflict on constraint uniq_operator_share_snapshots do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertOperatorShareSnapshots(snapshotDate string) error {
	forks, err := r.globalConfig.GetRewardsSqlForkDates()
	if err != nil {
		r.logger.Sugar().Errorw("Failed to get rewards fork dates", "error", err)
		return err
	}

	// Get the block number for the cutoff date to make block-based fork decision
	var cutoffBlockNumber uint64
	err = r.grm.Raw(`
		SELECT number
		FROM blocks
		WHERE block_time <= ?
		ORDER BY number DESC
		LIMIT 1
	`, snapshotDate).Scan(&cutoffBlockNumber).Error
	if err != nil {
		r.logger.Sugar().Errorw("Failed to get cutoff block number", "error", err, "snapshotDate", snapshotDate)
		return err
	}

	// Use block-based fork check for backwards compatibility
	useSabineFork := false
	if sabineFork, exists := forks[config.RewardsFork_Sabine]; exists {
		useSabineFork = cutoffBlockNumber >= sabineFork.BlockNumber
	}

	query, err := rewardsUtils.RenderQueryTemplate(operatorShareSnapshotsQuery, map[string]interface{}{
		"cutoffDate":            snapshotDate,
		"useSabineFork":         useSabineFork,
		"withdrawalQueueWindow": r.globalConfig.Rewards.WithdrawalQueueWindow,
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
