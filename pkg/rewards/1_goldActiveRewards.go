package rewards

import "database/sql"

var _1_goldActiveRewardsQuery = `
WITH active_rewards_modified as (
    SELECT *,
           amount/(duration/86400) as tokens_per_day,
           DATETIME(@cutoffDate) as global_end_inclusive -- Inclusive means we DO USE this day as a snapshot
    FROM combined_rewards
        WHERE end_timestamp >= TIMESTAMP @rewardsStart and start_timestamp <= TIMESTAMP @cutoffDate
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
             MIN(global_end_inclusive, end_timestamp) as reward_end_inclusive,
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
             CASE
                 WHEN '{{ var("is_backfill") }}' = 'true' THEN ap.reward_start_exclusive
                 ELSE COALESCE(MAX(g.snapshot), ap.reward_start_exclusive)
                 END as reward_start_exclusive,
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
                  LEFT JOIN {{ var('schema_name') }}.gold_table g 
                            ON g.reward_hash = ap.reward_hash
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
         SELECT * FROM active_reward_ranges
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

func (r *RewardsCalculator) GenerateActiveRewards(snapshotDate string) error {
	res := r.calculationDB.Exec(_1_goldActiveRewardsQuery, sql.Named("snapshotDate", snapshotDate))
	if res.Error != nil {
		return res.Error
	}
	return nil
}
