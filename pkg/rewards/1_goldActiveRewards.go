package rewards

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

var _1_goldActiveRewardsQuery = `
insert into rewards_gold_1_active_rewards (avs, snapshot, token, tokens_per_day, tokens_per_day_decimal, multiplier, strategy, reward_hash, reward_type, reward_submission_date)
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
select * from active_rewards_final
`

// Generate1ActiveRewards generates active rewards for the gold_1_active_rewards table
//
// @param snapshotDate: The upper bound of when to calculate rewards to
// @param startDate: The lower bound of when to calculate rewards from. If we're running rewards for the first time,
// this will be "1970-01-01". If this is a subsequent run, this will be the last snapshot date.
func (r *RewardsCalculator) Generate1ActiveRewards(snapshotDate string) error {
	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_1_ActiveRewards]

	rewardsStart := "1970-01-01 00:00:00" // This will always start as this date and get's updated later in the query

	r.logger.Sugar().Infow("Generating active rewards",
		zap.String("rewardsStart", rewardsStart),
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_1_goldActiveRewardsQuery, map[string]interface{}{
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
		r.logger.Sugar().Errorw("Failed to generate active rewards", "error", res.Error)
		return res.Error
	}
	return nil
}
