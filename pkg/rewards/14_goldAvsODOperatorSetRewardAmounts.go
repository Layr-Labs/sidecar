package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _14_goldAvsODOperatorSetRewardAmountsQuery = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Get the rows where operators have not registered for the AVS or if the AVS does not exist
WITH reward_snapshot_operators AS (
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
        operator_set_id,
        operator,
        SUM(tokens_per_registered_snapshot_decimal) OVER (PARTITION BY reward_hash, snapshot) AS avs_tokens
    FROM distinct_operators
)

-- Step 4: Output the final table
SELECT * FROM operator_token_sums
`

func (rc *RewardsCalculator) GenerateGold14AvsODOperatorSetRewardAmountsTable(snapshotDate string) error {
	rewardsV2_1Enabled, err := rc.globalConfig.IsRewardsV2_1EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}
	if !rewardsV2_1Enabled {
		rc.logger.Sugar().Infow("Rewards v2.1 is not enabled for this cutoff date, skipping GenerateGold14AvsODOperatorSetRewardAmountsTable")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_14_AvsODOperatorSetRewardAmounts]

	rc.logger.Sugar().Infow("Generating Avs OD operator set reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_14_goldAvsODOperatorSetRewardAmountsQuery, map[string]interface{}{
		"destTableName":        destTableName,
		"activeODRewardsTable": allTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_avs_od_operator_set_reward_amounts", "error", res.Error)
		return res.Error
	}
	return nil
}
