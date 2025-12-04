package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const stakerShareSnapshotsQuery = `
	insert into staker_share_snapshots(staker, strategy, shares, snapshot)
	WITH ranked_staker_records as (
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
	-- Get the range for each operator, strategy pairing
	staker_share_windows as (
	 SELECT
		 staker, strategy, shares, snapshot_time as start_time,
		 CASE
			 -- If the range does not have the end, use the current timestamp truncated to 0 UTC
			 WHEN LEAD(snapshot_time) OVER (PARTITION BY staker, strategy ORDER BY snapshot_time) is null THEN date_trunc('day', TIMESTAMP '{{.cutoffDate}}')
			 ELSE LEAD(snapshot_time) OVER (PARTITION BY staker, strategy ORDER BY snapshot_time)
			 END AS end_time
	 FROM snapshotted_records
	),
	cleaned_records as (
	  SELECT * FROM staker_share_windows
	  WHERE start_time < end_time
	),
	base_snapshots as (
		SELECT
			staker,
			strategy,
			shares,
			cast(day AS DATE) AS snapshot
		FROM
			cleaned_records
		CROSS JOIN
			generate_series(DATE(start_time), DATE(end_time) - interval '1' day, interval '1' day) AS day
	),
	-- Materialize withdrawal queue adjustments into final snapshots
	-- Per Sean's feedback: "Why not adjust the query that populates the staker_share_snapshots table to account for this case?"
	-- This materializes the combined result (base + withdrawal queue) into staker_share_snapshots
	withdrawal_queue_adjustments as (
		SELECT
			wqs.staker,
			wqs.strategy,
			wqs.shares,
			wqs.snapshot
		FROM withdrawal_queue_share_snapshots wqs
		WHERE wqs.snapshot = DATE '{{.snapshotDate}}'
	),
	combined_snapshots as (
		SELECT
			coalesce(base.staker, wq.staker) as staker,
			coalesce(base.strategy, wq.strategy) as strategy,
			(coalesce(base.shares::numeric, 0) + coalesce(wq.shares::numeric, 0))::text as shares,
			coalesce(base.snapshot, wq.snapshot) as snapshot
		FROM base_snapshots base
		FULL OUTER JOIN withdrawal_queue_adjustments wq
			ON base.staker = wq.staker
			AND base.strategy = wq.strategy
			AND base.snapshot = wq.snapshot
	)
	SELECT * FROM combined_snapshots
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
