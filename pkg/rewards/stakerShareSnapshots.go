package rewards

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const stakerShareSnapshotsQuery = `
	insert into staker_share_snapshots(staker, strategy, shares, snapshot)
	WITH
	-- Bronze sources: combine staker_shares with withdrawal queue
	ranked_staker_records as (
		SELECT *,
			   ROW_NUMBER() OVER (PARTITION BY staker, strategy, cast(block_time AS DATE) ORDER BY block_time DESC, log_index DESC) AS rn
		FROM staker_shares
		-- pipeline bronze table uses this to filter the correct records
		where block_time < TIMESTAMP '{{.cutoffDate}}'
	),
	-- Get the latest record for each day & round up to the snapshot day
	snapshotted_records as (
	 SELECT
		 staker,
		 strategy,
		 shares,
		 block_time,
		 date_trunc('day', block_time) + INTERVAL '1' day AS snapshot_time
	 from ranked_staker_records
	 where rn = 1
	),
	{{ if .useSabineFork }}
	-- Pre-aggregate withdrawal queue adjustments at bronze level (Post-Sabine fork only)
	cutoff_block as (
		SELECT number as cutoff_block_number
		FROM blocks
		WHERE block_time <= TIMESTAMP '{{.cutoffDate}}'
		ORDER BY number DESC
		LIMIT 1
	),
	queued_withdrawal_adjustments as (
		SELECT
			qsw.staker,
			qsw.strategy,
			qsw.block_number as withdrawal_block_number,
			b_queued.block_time as queued_time,
			-- Calculate effective shares after slashing: base shares * cumulative slash multiplier
			COALESCE(qsw.shares_to_withdraw::numeric, 0) * COALESCE(
				-- Get the latest (most recent) slash multiplier for this withdrawal
				(SELECT adj.slash_multiplier::numeric
				 FROM queued_withdrawal_slashing_adjustments adj
				 WHERE adj.staker = qsw.staker
				 AND adj.strategy = qsw.strategy
				 AND adj.withdrawal_block_number = qsw.block_number
				 AND adj.slash_block_number <= (SELECT cutoff_block_number FROM cutoff_block)
				 ORDER BY adj.slash_block_number DESC
				 LIMIT 1),
				1  -- No slashing if no adjustment records found
			) as effective_shares
		FROM queued_slashing_withdrawals qsw
		INNER JOIN blocks b_queued ON qsw.block_number = b_queued.number
		WHERE
			-- Still within withdrawal queue window (not yet completable)
			b_queued.block_time + INTERVAL '{{.withdrawalQueueWindow}} days' > TIMESTAMP '{{.snapshotDate}}'
			-- Backwards compatibility: only process records with valid data
			AND qsw.staker IS NOT NULL
			AND qsw.strategy IS NOT NULL
			AND qsw.operator IS NOT NULL
			AND qsw.shares_to_withdraw IS NOT NULL
			AND b_queued.block_time IS NOT NULL
	),
	{{ end }}
	-- Join bronze tables: base shares + pre-aggregated withdrawal adjustments
	-- This follows the design principle: "join bronze → snapshot" not "snapshot → inject adjustments"
	staker_shares_with_queue_bronze as (
		SELECT
			sr.staker,
			sr.strategy,
			{{ if .useSabineFork }}
			-- Post-Sabine fork: Add back queued withdrawals that are active during this snapshot's window
			(sr.shares::numeric + COALESCE(
				(SELECT SUM(qwa.effective_shares)
				 FROM queued_withdrawal_adjustments qwa
				 WHERE qwa.staker = sr.staker
				 AND qwa.strategy = sr.strategy
				 AND qwa.queued_time <= sr.block_time
				), 0
			))::numeric as shares,
			{{ else }}
			-- Pre-Sabine fork: Use base shares only (old behavior)
			sr.shares::numeric as shares,
			{{ end }}
			sr.snapshot_time
		FROM snapshotted_records sr
	),
	-- Get the range for each staker, strategy pairing
	staker_share_windows as (
	 SELECT
		 staker, strategy, shares, snapshot_time as start_time,
		 CASE
			 -- If the range does not have the end, use the current timestamp truncated to 0 UTC
			 WHEN LEAD(snapshot_time) OVER (PARTITION BY staker, strategy ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{.cutoffDate}}')
			 ELSE LEAD(snapshot_time) OVER (PARTITION BY staker, strategy ORDER BY snapshot_time)
			 END AS end_time
	 FROM staker_shares_with_queue_bronze
	),
	cleaned_records as (
	  SELECT * FROM staker_share_windows
	  WHERE start_time < end_time
	)
	-- Expand windows to daily snapshots
	SELECT
		staker,
		strategy,
		shares,
		cast(day AS DATE) AS snapshot
	FROM cleaned_records
	CROSS JOIN generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
	on conflict on constraint uniq_staker_share_snapshots do nothing;
`

func (r *RewardsCalculator) GenerateAndInsertStakerShareSnapshots(snapshotDate string) error {
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

	query, err := rewardsUtils.RenderQueryTemplate(stakerShareSnapshotsQuery, map[string]interface{}{
		"cutoffDate":            snapshotDate,
		"snapshotDate":          snapshotDate,
		"useSabineFork":         useSabineFork,
		"withdrawalQueueWindow": r.globalConfig.Rewards.WithdrawalQueueWindow,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate staker_share_snapshots",
			zap.String("snapshotDate", snapshotDate),
			zap.Error(res.Error),
		)
		return res.Error
	}
	return nil
}

func (r *RewardsCalculator) ListStakerShareSnapshots() ([]*StakerShareSnapshot, error) {
	var snapshots []*StakerShareSnapshot
	res := r.grm.Model(&StakerShareSnapshot{}).Find(&snapshots)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to list staker share snapshots", "error", res.Error)
		return nil, res.Error
	}
	return snapshots, nil
}
