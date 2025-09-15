package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _14_goldAvsODOperatorSetRewardAmountsQuery = `
INSERT INTO {{.destTableName}} (reward_hash, snapshot, token, avs, operator_set_id, operator, avs_tokens, generated_rewards_snapshot_id)

-- Step 1: Get the rows where operators have not registered for the AVS or if the AVS does not exist
WITH not_registered_operators AS (
    SELECT
        ap.reward_hash,
        ap.snapshot AS snapshot,
        ap.token,
        ap.tokens_per_registered_snapshot_decimal,
        ap.avs AS avs,
        ap.operator_set_id AS operator_set_id,
        ap.operator AS operator,
        ap.strategy,
        ap.multiplier,
        ap.reward_submission_date
    FROM {{.activeODRewardsTable}} ap
    WHERE ap.generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
      AND ap.num_registered_snapshots = 0
),

-- Step 2: Dedupe the operator tokens across strategies for each (operator, reward hash, snapshot)
-- Since the above result is a flattened operator-directed reward submission across strategies
distinct_not_registered_operators AS (
    SELECT *
    FROM (
        SELECT 
            *,
            -- We can use an arbitrary order here since the avs_tokens is the same for each (operator, strategy, hash, snapshot)
            -- We use strategy ASC for better debuggability
            ROW_NUMBER() OVER (
                PARTITION BY reward_hash, snapshot, operator 
                ORDER BY strategy ASC
            ) AS rn
        FROM not_registered_operators
    ) t
    WHERE rn = 1
),

-- Step 3: Sum the operator tokens for each (reward hash, snapshot)
-- Since we want to refund the sum of those operator amounts to the AVS in that reward submission for that snapshot
avs_operator_refund_sums AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        avs,
        operator_set_id,
        operator,
        SUM(tokens_per_registered_snapshot_decimal) OVER (PARTITION BY reward_hash, snapshot) AS avs_tokens
    FROM distinct_not_registered_operators
),

-- Step 4: Find rows where operators are registered but strategies are not registered for the operator set
-- First, get all rows where operators are registered
registered_operators AS (
    SELECT
        ap.reward_hash,
        ap.snapshot,
        ap.token,
        ap.tokens_per_registered_snapshot_decimal,
        ap.avs,
        ap.operator_set_id,
        ap.operator,
        ap.strategy,
        ap.multiplier,
        ap.reward_submission_date
    FROM {{.activeODRewardsTable}} ap
    JOIN operator_set_operator_registration_snapshots osor
        ON ap.avs = osor.avs 
        AND ap.operator_set_id = osor.operator_set_id
        AND ap.snapshot = osor.snapshot 
        AND ap.operator = osor.operator
    WHERE ap.generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
      AND ap.num_registered_snapshots != 0
      AND ap.reward_submission_date >= @coloradoHardforkDate
),

-- Step 5: For each reward/snapshot/operator_set, check if any strategies are registered
strategies_registered AS (
    SELECT DISTINCT
        ro.reward_hash,
        ro.snapshot,
        ro.avs,
        ro.operator_set_id
    FROM registered_operators ro
    JOIN operator_set_strategy_registration_snapshots ossr
        ON ro.avs = ossr.avs
        AND ro.operator_set_id = ossr.operator_set_id
        AND ro.snapshot = ossr.snapshot
        AND ro.strategy = ossr.strategy
),

-- Step 6: Find reward/snapshot combinations where operators registered but no strategies registered
strategies_not_registered AS (
    SELECT 
        ro.*
    FROM registered_operators ro
    LEFT JOIN strategies_registered sr
        ON ro.reward_hash = sr.reward_hash
        AND ro.snapshot = sr.snapshot
        AND ro.avs = sr.avs
        AND ro.operator_set_id = sr.operator_set_id
    WHERE sr.reward_hash IS NULL
),

-- Step 7: Calculate the staker split for each reward with dynamic split logic
-- If no split is found, default to 1000 (10%)
staker_splits AS (
    SELECT 
        snr.*,
        snr.tokens_per_registered_snapshot_decimal - FLOOR(snr.tokens_per_registered_snapshot_decimal * COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS DECIMAL)) AS staker_split
    FROM strategies_not_registered snr
    LEFT JOIN operator_set_split_snapshots oss
        ON snr.operator = oss.operator 
        AND snr.avs = oss.avs 
        AND snr.operator_set_id = oss.operator_set_id
        AND snr.snapshot = oss.snapshot
    LEFT JOIN default_operator_split_snapshots dos ON (snr.snapshot = dos.snapshot)
),

-- Step 8: Dedupe the staker splits across across strategies for each (operator, reward hash, snapshot)
-- Since the above result is a flattened operator-directed reward submission across strategies.
distinct_staker_splits AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY reward_hash, snapshot, operator 
                ORDER BY strategy ASC
            ) AS rn
        FROM staker_splits
    ) t
    WHERE rn = 1
),

-- Step 9: Sum the staker tokens for each (reward hash, snapshot) that should be refunded
avs_staker_refund_sums AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        avs,
        operator_set_id,
        operator,
        SUM(staker_split) OVER (PARTITION BY reward_hash, snapshot) AS avs_tokens
    FROM distinct_staker_splits
),

-- Step 10: Combine both refund cases into one result
combined_avs_refund_amounts AS (
    SELECT * FROM avs_operator_refund_sums
    UNION ALL
    SELECT * FROM avs_staker_refund_sums
)

-- Output the final table
SELECT *, {{.generatedRewardsSnapshotId}} as generated_rewards_snapshot_id FROM combined_avs_refund_amounts
ON CONFLICT (reward_hash, snapshot, operator_set_id, operator, token) DO NOTHING
`

func (rc *RewardsCalculator) GenerateGold14AvsODOperatorSetRewardAmountsTable(snapshotDate string, generatedRewardsSnapshotId uint64, forks config.ForkMap) error {
	rewardsV2_1Enabled, err := rc.globalConfig.IsRewardsV2_1EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}
	if !rewardsV2_1Enabled {
		rc.logger.Sugar().Infow("Rewards v2.1 is not enabled for this cutoff date, skipping GenerateGold14AvsODOperatorSetRewardAmountsTable")
		return nil
	}

	destTableName := rewardsUtils.RewardsTable_14_AvsODOperatorSetRewardAmounts
	activeODRewardsTable := rc.getTempActiveODOperatorSetRewardsTableName(snapshotDate, generatedRewardsSnapshotId)

	rc.logger.Sugar().Infow("Generating Avs OD operator set reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("coloradoHardforkDate", forks[config.RewardsFork_Colorado].Date),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_14_goldAvsODOperatorSetRewardAmountsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"activeODRewardsTable":       activeODRewardsTable,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query, sql.Named("coloradoHardforkDate", forks[config.RewardsFork_Colorado].Date))
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_avs_od_operator_set_reward_amounts", "error", res.Error)
		return res.Error
	}
	return nil
}
