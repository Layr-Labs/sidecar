package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

var _7_goldActiveODRewardsQuery = `
CREATE TABLE {{.destTableName}} AS
WITH 
-- Step 2: Modify active rewards and compute tokens per day
active_rewards_modified AS (
    SELECT 
        *,
        amount / (duration / 86400) AS tokens_per_day,
        CAST(@cutoffDate AS TIMESTAMP(6)) AS global_end_inclusive -- Inclusive means we DO USE this day as a snapshot
    FROM operator_directed_rewards
    WHERE end_timestamp >= TIMESTAMP '{{.rewardsStart}}'
      AND start_timestamp <= TIMESTAMP '{{.cutoffDate}}'
      AND block_time <= TIMESTAMP '{{.cutoffDate}}' -- Always ensure we're not using future data. Should never happen since we're never backfilling, but here for safety and consistency.
),

-- Step 3: Cut each reward's start and end windows to handle the global range
active_rewards_updated_end_timestamps AS (
    SELECT
        avs,
        operator,
        /**
         * Cut the start and end windows to handle
         * A. Retroactive rewards that came recently whose start date is less than start_timestamp
         * B. Don't make any rewards past end_timestamp for this run
         */
        start_timestamp AS reward_start_exclusive,
        LEAST(global_end_inclusive, end_timestamp) AS reward_end_inclusive,
        tokens_per_day,
        token,
        multiplier,
        strategy,
        reward_hash,
        global_end_inclusive,
        block_date AS reward_submission_date
    FROM active_rewards_modified
),

-- Step 4: For each reward hash, find the latest snapshot
active_rewards_updated_start_timestamps AS (
    SELECT
        ap.avs,
        ap.operator,
        COALESCE(MAX(g.snapshot), ap.reward_start_exclusive) AS reward_start_exclusive,
        ap.reward_end_inclusive,
        ap.token,
        -- We use floor to ensure we are always underesimating total tokens per day
        FLOOR(ap.tokens_per_day) AS tokens_per_day_decimal,
        -- Round down to 15 sigfigs for double precision, ensuring know errouneous round up or down
        ap.tokens_per_day * ((POW(10, 15) - 1) / POW(10, 15)) AS tokens_per_day,
        ap.multiplier,
        ap.strategy,
        ap.reward_hash,
        ap.global_end_inclusive,
        ap.reward_submission_date
    FROM active_rewards_updated_end_timestamps ap
    LEFT JOIN gold_table g 
        ON g.reward_hash = ap.reward_hash
    GROUP BY 
        ap.avs, 
        ap.operator, 
        ap.reward_end_inclusive, 
        ap.token, 
        ap.tokens_per_day, 
        ap.multiplier, 
        ap.strategy, 
        ap.reward_hash, 
        ap.global_end_inclusive, 
        ap.reward_start_exclusive, 
        ap.reward_submission_date
),

-- Step 5: Filter out invalid reward ranges
active_reward_ranges AS (
    /** Take out (reward_start_exclusive, reward_end_inclusive) windows where
	* 1. reward_start_exclusive >= reward_end_inclusive: The reward period is done or we will handle on a subsequent run
	*/
    SELECT * 
    FROM active_rewards_updated_start_timestamps
    WHERE reward_start_exclusive < reward_end_inclusive
),

-- Step 6: Explode out the ranges for a day per inclusive date
exploded_active_range_rewards AS (
    SELECT
        *
    FROM active_reward_ranges
    CROSS JOIN generate_series(
        DATE(reward_start_exclusive), 
        DATE(reward_end_inclusive), 
        INTERVAL '1' DAY
    ) AS day
),

-- Step 7: Prepare final active rewards
active_rewards_final AS (
    SELECT
        avs,
        operator,
        CAST(day AS DATE) AS snapshot,
        token,
        tokens_per_day,
        tokens_per_day_decimal,
        multiplier,
        strategy,
        reward_hash,
        reward_submission_date
    FROM exploded_active_range_rewards
    -- Remove snapshots on the start day
    WHERE day != reward_start_exclusive
)

SELECT * FROM active_rewards_final
`

// Generate7ActiveODRewards generates active operator-directed rewards for the gold_7_active_od_rewards table
//
// @param snapshotDate: The upper bound of when to calculate rewards to
// @param startDate: The lower bound of when to calculate rewards from. If we're running rewards for the first time,
// this will be "1970-01-01". If this is a subsequent run, this will be the last snapshot date.
func (r *RewardsCalculator) Generate7ActiveODRewards(snapshotDate string) error {
	rewardsV2Enabled, err := r.globalConfig.IsRewardsV2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}
	if !rewardsV2Enabled {
		r.logger.Sugar().Infow("Rewards v2 is not enabled for this cutoff date, skipping Generate7ActiveODRewards")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_7_ActiveODRewards]

	rewardsStart := "1970-01-01 00:00:00" // This will always start as this date and get's updated later in the query

	r.logger.Sugar().Infow("Generating active rewards",
		zap.String("rewardsStart", rewardsStart),
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_7_goldActiveODRewardsQuery, map[string]interface{}{
		"destTableName": destTableName,
		"rewardsStart":  rewardsStart,
		"cutoffDate":    snapshotDate,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := r.grm.Exec(query,
		sql.Named("cutoffDate", snapshotDate),
	)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate active od rewards", "error", res.Error)
		return res.Error
	}
	return nil
}
