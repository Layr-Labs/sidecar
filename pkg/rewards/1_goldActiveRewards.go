package rewards

import (
	"database/sql"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"

	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

var _1_goldActiveRewardsQuery = `
create table {{.destTableName}} as
WITH active_rewards_modified as (
    SELECT *,
           amount/(duration/86400) as tokens_per_day,
           cast(@cutoffDate AS TIMESTAMP(6)) as global_end_inclusive -- Inclusive means we DO USE this day as a snapshot
    FROM combined_rewards
        WHERE
    		end_timestamp >= TIMESTAMP '{{.rewardsStart}}'
    		and start_timestamp <= TIMESTAMP '{{.cutoffDate}}'
    		and block_time <= TIMESTAMP '{{.cutoffDate}}' -- Always ensure we're not using future data. Should never happen since we're never backfilling, but here for safety and consistency.
),
-- Cut each reward's start and end windows to handle the global range
active_rewards_updated_end_timestamps as (
 SELECT
	 avs,
	 /**
	  * Cut the start and end windows to handle
	  * A. Retroactive rewards that came recently whose start date is less than start_timestamp
	  * B. Don't make any rewards past end_timestamp for this run
	  */
	 start_timestamp as reward_start_exclusive,
	 LEAST(global_end_inclusive, end_timestamp) as reward_end_inclusive,
	 tokens_per_day,
	 token,
	 multiplier,
	 strategy,
	 reward_hash,
	 reward_type,
	 global_end_inclusive,
	 block_date as reward_submission_date
 FROM active_rewards_modified
),
-- For each reward hash, find the latest snapshot
active_rewards_updated_start_timestamps as (
	SELECT
		ap.avs,
		COALESCE(MAX(g.snapshot), ap.reward_start_exclusive) as reward_start_exclusive,
		-- ap.reward_start_exclusive,
		ap.reward_end_inclusive,
		ap.token,
		-- We use floor to ensure we are always underesimating total tokens per day
		floor(ap.tokens_per_day) as tokens_per_day_decimal,
		-- Round down to 15 sigfigs for double precision, ensuring know errouneous round up or down
		ap.tokens_per_day * ((POW(10, 15) - 1)/(POW(10, 15))) as tokens_per_day,
		ap.multiplier,
		ap.strategy,
		ap.reward_hash,
		ap.reward_type,
		ap.global_end_inclusive,
	 	ap.reward_submission_date
	FROM active_rewards_updated_end_timestamps ap
	LEFT JOIN gold_table g ON g.reward_hash = ap.reward_hash
 GROUP BY ap.avs, ap.reward_end_inclusive, ap.token, ap.tokens_per_day, ap.multiplier, ap.strategy, ap.reward_hash, ap.global_end_inclusive, ap.reward_start_exclusive, ap.reward_type, ap.reward_submission_date
),
-- Parse out invalid ranges
active_reward_ranges AS (
	SELECT * from active_rewards_updated_start_timestamps
	/** Take out (reward_start_exclusive, reward_end_inclusive) windows where
	* 1. reward_start_exclusive >= reward_end_inclusive: The reward period is done or we will handle on a subsequent run
	*/
	WHERE reward_start_exclusive < reward_end_inclusive
),
-- Explode out the ranges for a day per inclusive date
exploded_active_range_rewards AS (
	SELECT
	    *
	FROM active_reward_ranges
	CROSS JOIN generate_series(DATE(reward_start_exclusive), DATE(reward_end_inclusive), INTERVAL '1' DAY) AS day
),
active_rewards_final AS (
	SELECT
		avs,
		cast(day as DATE) as snapshot,
		token,
		tokens_per_day,
		tokens_per_day_decimal,
		multiplier,
		strategy,
		reward_hash,
		reward_type,
		reward_submission_date
	FROM exploded_active_range_rewards
	-- Remove snapshots on the start day
	WHERE day != reward_start_exclusive
)
select
    arf.*,
    {{.generatedRewardsSnapshotId}} as generated_rewards_snapshot_id
from active_rewards_final as arf
`

// Generate1ActiveRewards generates active rewards for the gold_1_active_rewards table
//
// @param snapshotDate: The upper bound of when to calculate rewards to
// @param startDate: The lower bound of when to calculate rewards from. If we're running rewards for the first time,
// this will be "1970-01-01". If this is a subsequent run, this will be the last snapshot date.
func (r *RewardsCalculator) Generate1ActiveRewards(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	// destTableName := rewardsUtils.RewardsTable_1_ActiveRewards
	destTableName := r.getTempActiveRewardsTableName(snapshotDate, generatedRewardsSnapshotId)

	// first, drop the table if it already exists
	if err := r.DropTempActiveRewardsTable(snapshotDate, generatedRewardsSnapshotId); err != nil {
		r.logger.Sugar().Errorw("Failed to drop existing temp active rewards table", "error", err)
		return err
	}

	rewardsStart := "1970-01-01 00:00:00" // This will always start as this date and get's updated later in the query

	r.logger.Sugar().Infow("Generating active rewards",
		zap.String("rewardsStart", rewardsStart),
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_1_goldActiveRewardsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"rewardsStart":               rewardsStart,
		"cutoffDate":                 snapshotDate,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	// query = query + " ON CONFLICT (avs, reward_hash, strategy, snapshot) DO NOTHING"

	res := r.grm.Exec(query,
		sql.Named("cutoffDate", snapshotDate),
		sql.Named("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate active rewards", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) getTempActiveRewardsTableName(snapshotDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(snapshotDate)
	return fmt.Sprintf("tmp_rewards_gold_1_active_rewards_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (r *RewardsCalculator) DropTempActiveRewardsTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := r.getTempActiveRewardsTableName(snapshotDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := r.grm.Exec(query)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to drop temp active rewards table", "error", res.Error)
		return res.Error
	}
	r.logger.Sugar().Infow("Successfully dropped temp active rewards table",
		zap.String("tempTableName", tempTableName),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	return nil
}

func (r *RewardsCalculator) CopyTempActiveRewardsToActiveRewards(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	r.logger.Sugar().Infow("Copying temp active rewards to active rewards",
		zap.String("snapshotDate", snapshotDate),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	tempTableName := r.getTempActiveRewardsTableName(snapshotDate, generatedRewardsSnapshotId)
	destTableName := rewardsUtils.RewardsTable_1_ActiveRewards

	query := `
		insert into {{.destTableName}} (avs, snapshot, token, tokens_per_day, tokens_per_day_decimal, multiplier, strategy, reward_hash, reward_type, reward_submission_date, generated_rewards_snapshot_id)
		select avs, snapshot, token, tokens_per_day, tokens_per_day_decimal, multiplier, strategy, reward_hash, reward_type, reward_submission_date, generated_rewards_snapshot_id from {{.tempTableName}}
		on conflict (avs, reward_hash, strategy, snapshot) do nothing
	`
	renderedQuery, err := rewardsUtils.RenderQueryTemplate(query, map[string]interface{}{
		"destTableName": destTableName,
		"tempTableName": tempTableName,
	})
	if err != nil {
		return fmt.Errorf("failed to render query template: %w", err)
	}
	res := r.grm.Exec(renderedQuery)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to copy temp active rewards to active rewards", "error", res.Error)
		return res.Error
	}

	return r.DropTempActiveRewardsTable(snapshotDate, generatedRewardsSnapshotId)
}
