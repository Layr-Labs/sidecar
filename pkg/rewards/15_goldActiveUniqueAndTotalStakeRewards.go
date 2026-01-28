package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _15_goldActiveUniqueAndTotalStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS
WITH 
-- Step 1: Modify active rewards and compute tokens per day for stake submissions
active_rewards_modified AS (
    SELECT 
        avs,
        operator_set_id,
        reward_hash,
        token,
        amount,
        strategy,
        strategy_index,
        multiplier,
        start_timestamp,
        end_timestamp,
        duration,
        reward_type,
        block_number,
        block_time,
        block_date,
        CAST('{{.cutoffDate}}' AS TIMESTAMP(6)) AS global_end_inclusive
    FROM stake_operator_set_rewards
    WHERE end_timestamp >= TIMESTAMP '{{.rewardsStart}}'
      AND start_timestamp <= TIMESTAMP '{{.cutoffDate}}'
      AND block_time <= TIMESTAMP '{{.cutoffDate}}'
),

-- Step 2: Cut each reward's start and end windows to handle the global range
active_rewards_updated_end_timestamps AS (
    SELECT
        avs,
        operator_set_id,
        reward_hash,
        token,
        amount,
        strategy,
        strategy_index,
        multiplier,
        reward_type,
        start_timestamp AS reward_start_exclusive,
        LEAST(global_end_inclusive, end_timestamp) AS reward_end_inclusive,
        duration,
        global_end_inclusive,
        block_date AS reward_submission_date
    FROM active_rewards_modified
),

-- Step 3: For each reward hash, find the latest snapshot
active_rewards_updated_start_timestamps AS (
    SELECT
        ap.avs,
        ap.operator_set_id,
        ap.reward_hash,
        ap.token,
        ap.amount,
        ap.strategy,
        ap.strategy_index,
        ap.multiplier,
        ap.reward_type,
        COALESCE(MAX(g.snapshot), ap.reward_start_exclusive) AS reward_start_exclusive,
        ap.reward_end_inclusive,
        ap.duration,
        ap.global_end_inclusive,
        ap.reward_submission_date
    FROM active_rewards_updated_end_timestamps ap
    LEFT JOIN gold_table g 
        ON g.reward_hash = ap.reward_hash
    GROUP BY 
        ap.avs, 
        ap.operator_set_id,
        ap.reward_hash,
        ap.token,
        ap.amount,
        ap.strategy,
        ap.strategy_index,
        ap.multiplier,
        ap.reward_type,
        ap.reward_end_inclusive, 
        ap.duration,
        ap.global_end_inclusive, 
        ap.reward_start_exclusive, 
        ap.reward_submission_date
),

-- Step 4: Filter out invalid reward ranges
active_reward_ranges AS (
    SELECT * 
    FROM active_rewards_updated_start_timestamps
    WHERE reward_start_exclusive < reward_end_inclusive
),

-- Step 5: Explode out the ranges for a day per inclusive date
exploded_active_range_rewards AS (
    SELECT
        avs,
        operator_set_id,
        reward_hash,
        token,
        amount,
        strategy,
        strategy_index,
        multiplier,
        reward_type,
        reward_start_exclusive,
        reward_end_inclusive,
        duration,
        global_end_inclusive,
        reward_submission_date,
        -- This is the "snapshot" date - the day on which rewards are accrued
        d AS snapshot
    FROM active_reward_ranges
    CROSS JOIN generate_series(
        DATE(reward_start_exclusive), 
        DATE(reward_end_inclusive), 
        INTERVAL '1' day
    ) AS d
    WHERE d != reward_start_exclusive
),

-- Step 6: Calculate tokens per day
-- For stake-based rewards, tokens_per_day is the daily amount to be distributed pro-rata
tokens_per_day AS (
    SELECT
        *,
        -- Total tokens per day = floor(amount / duration_in_days)
        -- duration is in seconds, convert to days
        FLOOR(amount / (duration / 86400)) AS tokens_per_day_decimal
    FROM exploded_active_range_rewards
)

SELECT * FROM tokens_per_day
`

func (rc *RewardsCalculator) GenerateGold15ActiveUniqueAndTotalStakeRewardsTable(snapshotDate string) error {
	rewardsV2_2Enabled, err := rc.globalConfig.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.2 is enabled", "error", err)
		return err
	}
	if !rewardsV2_2Enabled {
		rc.logger.Sugar().Infow("Rewards v2.2 is not enabled, skipping v2.2 table 15 (active unique and total stake rewards)")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_15_ActiveUniqueAndTotalStakeRewards]

	rewardsStart := "1970-01-01 00:00:00" // This will always start as this date and gets updated later in the query

	rc.logger.Sugar().Infow("Generating v2.2 Active unique and total stake rewards",
		zap.String("rewardsStart", rewardsStart),
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_15_goldActiveUniqueAndTotalStakeRewardsQuery, map[string]interface{}{
		"destTableName": destTableName,
		"rewardsStart":  rewardsStart,
		"cutoffDate":    snapshotDate,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_15_active_unique_and_total_stake_rewards v2.2", "error", res.Error)
		return res.Error
	}
	return nil
}
