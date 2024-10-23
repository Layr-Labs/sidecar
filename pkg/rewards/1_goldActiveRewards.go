package rewards

import (
	"database/sql"
)

var _1_goldActiveRewardsQuery = `
insert into gold_1_active_rewards
WITH active_rewards_modified as (
	SELECT
		*,
		tokens_per_day(amount, duration) as tokens_per_day,
		tokens_per_day_decimal(amount, duration) as tokens_per_day_decimal,
		DATETIME(@cutoffDate) as global_end_inclusive -- Inclusive means we DO USE this day as a snapshot
	FROM combined_rewards
	WHERE
	    end_timestamp >= DATETIME(@rewardsStart)
	  	and start_timestamp <= DATETIME(@cutoffDate)
	  	-- since we cant do backfills, each run will need to be incremental and use the 
	    -- block_date as an upper bound
		and block_date <= DATETIME(@cutoffDate)
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
		tokens_per_day_decimal,
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
		coalesce(MAX(DATE(g.snapshot)), DATE(ap.reward_start_exclusive)) as reward_start_exclusive,
		ap.reward_end_inclusive,
		ap.token,
		ap.multiplier,
		ap.strategy,
		ap.reward_hash,
		ap.reward_type,
		ap.tokens_per_day,
		ap.tokens_per_day_decimal,
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
	from active_reward_ranges
),
day_series AS (
	with RECURSIVE day_series_inner AS (
		SELECT DATE(min_start) AS dayy
		FROM date_bounds
		UNION ALL
		SELECT DATE(dayy, '+1 day')
		FROM day_series_inner
		WHERE dayy < (SELECT max_end FROM date_bounds)
	)
	select * from day_series_inner
),
-- Explode out the ranges for a day per inclusive date
 exploded_active_range_rewards AS (
	 SELECT
		arr.*,
		day_series.dayy as dayy
	 FROM active_reward_ranges as arr
	 cross join day_series
	 where DATE(day_series.dayy) between DATE(reward_start_exclusive) and DATE(reward_end_inclusive)
 ),
 active_rewards_final AS (
	 SELECT
		 avs,
		 DATE(dayy) as snapshot,
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
	 WHERE DATE(dayy) != reward_start_exclusive
 )
select
	avs,
	snapshot,
	token,
	tokens_per_day,
	tokens_per_day_decimal,
	multiplier,
	strategy,
	reward_hash,
	reward_type,
	reward_submission_date
from active_rewards_final
where snapshot >= DATE(@startDate)
`

type ResultRow struct {
	Avs                 string
	Snapshot            string
	Token               string
	TokensPerDay        string
	TokensPerDayDecimal string
}

// Generate1ActiveRewards generates active rewards for the gold_1_active_rewards table
//
// @param startDate: The lower bound of when to calculate rewards from. If we're running rewards for the first time,
// this will be "1970-01-01". If this is a subsequent run, this will be the last snapshot date.
// @param cutoffDate: The upper bound of when to calculate rewards to (inclusive)
func (r *RewardsCalculator) Generate1ActiveRewards(startDate string, cutoffDate string) error {
	r.logger.Sugar().Infow("Generating active rewards", "cutoffDate", cutoffDate, "startDate", startDate)
	res := r.grm.Exec(_1_goldActiveRewardsQuery,
		sql.Named("cutoffDate", cutoffDate),
		sql.Named("startDate", startDate),
		sql.Named("rewardsStart", "1970-01-01"),
	)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate active rewards", "error", res.Error)
		return res.Error
	}
	return nil
}

func (r *RewardsCalculator) CreateGold1ActiveRewardsTable() error {
	queries := []string{
		`create table if not exists gold_1_active_rewards (
		avs TEXT NOT NULL,
		snapshot DATE NOT NULL,
		token TEXT NOT NULL,
		tokens_per_day TEXT NOT NULL,
		tokens_per_day_decimal TEXT NOT NULL,
		multiplier TEXT NOT NULL,
		strategy TEXT NOT NULL,
		reward_hash TEXT NOT NULL,
		reward_type TEXT NOT NULL,
		reward_submission_date DATE NOT NULL
	)`,
		`create index if not exists idx_gold_1_active_rewards_snapshot on gold_1_active_rewards(snapshot)`,
		`create index if not exists idx_gold_1_active_rewards_reward_type on gold_1_active_rewards(reward_type)`,
	}
	for _, query := range queries {
		res := r.grm.Exec(query)
		if res.Error != nil {
			r.logger.Sugar().Errorw("Failed to create gold_1_active_rewards table", "error", res.Error)
			return res.Error
		}
	}
	return nil
}
