package rewards

import (
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
	-- Join with withdrawal queue at bronze level before creating windows
	-- This ensures withdrawal queue adjustments are integrated into the time-series logic
	-- IMPORTANT: Accounts for slashing events via pre-computed adjustment table
	staker_shares_with_queue_bronze as (
		SELECT
			sr.staker,
			sr.strategy,
			-- Add back queued withdrawals that are active during this snapshot's window
			-- ADJUSTED for any slashing that occurred while in queue (from adjustment table)
			(sr.shares::numeric + COALESCE(
				(SELECT SUM(
					-- Effective withdrawal amount after applying all slashing
					COALESCE(qsw.shares_to_withdraw::numeric, 0) * COALESCE(
						-- Get the latest (most recent) slash multiplier for this withdrawal
						(SELECT adj.slash_multiplier::numeric
						 FROM queued_withdrawal_slashing_adjustments adj
						 WHERE adj.staker = qsw.staker
						 AND adj.strategy = qsw.strategy
						 AND adj.withdrawal_block_number = qsw.block_number
						 -- Only consider slashing that happened before the cutoff date
						 AND adj.slash_block_number <= (
							 SELECT number FROM blocks WHERE block_time <= TIMESTAMP '{{.cutoffDate}}' ORDER BY number DESC LIMIT 1
						 )
						 ORDER BY adj.slash_block_number DESC
						 LIMIT 1),
						1  -- No slashing if no adjustment records found
					)
				 )
				 FROM queued_slashing_withdrawals qsw
				 INNER JOIN blocks b_queued ON qsw.block_number = b_queued.number
				 WHERE qsw.staker = sr.staker
				 AND qsw.strategy = sr.strategy
				 AND b_queued.block_time <= sr.block_time
				 AND DATE(b_queued.block_time) + INTERVAL '14 days' > DATE '{{.snapshotDate}}'
				 -- Backwards compatibility: only process records with valid data
				 AND qsw.staker IS NOT NULL
				 AND qsw.strategy IS NOT NULL
				 AND qsw.operator IS NOT NULL
				 AND qsw.shares_to_withdraw IS NOT NULL
				 AND b_queued.block_time IS NOT NULL
				), 0
			))::text as shares,
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
	query, err := rewardsUtils.RenderQueryTemplate(stakerShareSnapshotsQuery, map[string]interface{}{
		"cutoffDate":   snapshotDate,
		"snapshotDate": snapshotDate,
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
