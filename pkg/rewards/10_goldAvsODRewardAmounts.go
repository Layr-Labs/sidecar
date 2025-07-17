package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _10_goldAvsODRewardAmountsQuery = `
INSERT INTO {{.destTableName}} (reward_hash, snapshot, token, avs, operator, avs_tokens, generated_rewards_snapshot_id)

-- Step 1: Get the rows where operators have not registered for the AVS or if the AVS does not exist
WITH reward_snapshot_operators AS (
    SELECT
        ap.reward_hash,
        ap.snapshot AS snapshot,
        ap.token,
        ap.tokens_per_registered_snapshot_decimal,
        ap.avs AS avs,
        ap.operator AS operator,
        ap.strategy,
        ap.multiplier,
        ap.reward_submission_date
    FROM {{.activeODRewardsTable}} ap
    WHERE
        ap.num_registered_snapshots = 0
),

-- Step 2: Dedupe the operator tokens across strategies for each (operator, reward hash, snapshot)
-- Since the above result is a flattened operator-directed reward submission across strategies
distinct_operators AS (
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
        FROM reward_snapshot_operators
    ) t
    WHERE rn = 1
),

-- Step 3: Sum the operator tokens for each (reward hash, snapshot)
-- Since we want to refund the sum of those operator amounts to the AVS in that reward submission for that snapshot
operator_token_sums AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        avs,
        operator,
        SUM(tokens_per_registered_snapshot_decimal) OVER (PARTITION BY reward_hash, snapshot) AS avs_tokens
    FROM distinct_operators
)

-- Step 4: Output the final table
SELECT *, {{.generatedRewardsSnapshotId}} as generated_rewards_snapshot_id FROM operator_token_sums
ON CONFLICT (reward_hash, avs, operator, snapshot) DO NOTHING
`

func (rc *RewardsCalculator) GenerateGold10AvsODRewardAmountsTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	rewardsV2Enabled, err := rc.globalConfig.IsRewardsV2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}
	if !rewardsV2Enabled {
		rc.logger.Sugar().Infow("Rewards v2 is not enabled for this cutoff date, skipping GenerateGold10AvsODRewardAmountsTable")
		return nil
	}

	destTableName := rewardsUtils.RewardsTable_10_AvsODRewardAmounts
	activeOdRewardsTableName := rc.getTempActiveODRewardsTableName(snapshotDate, generatedRewardsSnapshotId)

	rc.logger.Sugar().Infow("Generating Avs OD reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_10_goldAvsODRewardAmountsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"activeODRewardsTable":       activeOdRewardsTableName,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_avs_od_reward_amounts", "error", res.Error)
		return res.Error
	}
	return nil
}
