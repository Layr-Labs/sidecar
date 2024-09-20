package rewards

import "database/sql"

var _1_goldActiveRewardsQuery = `
create table active_rewards as 
WITH active_rewards_modified as (
	SELECT
		*,
		calc_raw_tokens_per_day(amount, duration) as tokens_per_day,
		DATETIME(@cutoffDate) as global_end_inclusive -- Inclusive means we DO USE this day as a snapshot
	FROM combined_rewards
	WHERE end_timestamp >= DATETIME(@rewardsStart) and start_timestamp <= DATETIME(@cutoffDate)
),
-- Cut each reward's start and end windows to handle the global range
active_rewards_updated_end_timestamps as (
	SELECT
		avs,
		-- Cut the start and end windows to handle
		-- A. Retroactive rewards that came recently whose start date is less than start_timestamp
		-- B. Don't make any rewards past end_timestamp for this run
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
		ap.reward_start_exclusive as reward_start_exclusive,
		ap.reward_end_inclusive,
		ap.token,
		post_nile_tokens_per_day(ap.tokens_per_day) as tokens_per_day_decimal,
		pre_nile_tokens_per_day(ap.tokens_per_day) as tokens_per_day,
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
	-- Take out (reward_start_exclusive, reward_end_inclusive) windows where
	-- 1. reward_start_exclusive >= reward_end_inclusive: The reward period is done or we will handle on a subsequent run
	WHERE reward_start_exclusive < reward_end_inclusive
),
	date_bounds as (
		select
			min(reward_start_exclusive) as min_start,
			max(reward_end_inclusive) as max_end
		from operator_avs_registration_windows
	),
	day_series AS (
		with RECURSIVE day_series_inner AS (
			SELECT DATE(min_start) AS day
			FROM date_bounds
			UNION ALL
			SELECT DATE(day, '+1 day')
			FROM day_series_inner
			WHERE day < (SELECT max_end FROM date_bounds)
		)
		select * from day_series_inner
	),
-- Explode out the ranges for a day per inclusive date
     exploded_active_range_rewards AS (
         SELECT
         	arr.*,
         	day_series.day as day
         FROM active_reward_ranges as arr
		 cross join day_series
		 where DATE(day_series.day) between DATE(reward_start_exclusive) and DATE(reward_end_inclusive)
     ),
     active_rewards_final AS (
         SELECT
             avs,
             DATE(day) as snapshot,
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
	res := r.calculationDB.Exec(_1_goldActiveRewardsQuery,
		sql.Named("snapshotDate", snapshotDate),
		sql.Named("rewardsStart", "1970-01-01"),
	)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate active rewards", "error", res.Error)
		return res.Error
	}
	return nil
}
