package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _12_goldOperatorODOperatorSetRewardAmountsQuery = `
INSERT INTO {{.destTableName}} (reward_hash, snapshot, token, tokens_per_registered_snapshot_decimal, avs, operator_set_id, operator, strategy, multiplier, reward_submission_date, rn, split_pct, operator_tokens, generated_rewards_snapshot_id)

-- Step 1: Get the rows where operators have registered for the operator set
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
    JOIN operator_set_operator_registration_snapshots osor
        ON ap.avs = osor.avs 
       AND ap.operator_set_id = osor.operator_set_id
       AND ap.snapshot = osor.snapshot 
       AND ap.operator = osor.operator
),

-- Step 2: Dedupe the operator tokens across strategies for each (operator, reward hash, snapshot)
-- Since the above result is a flattened operator-directed reward submission across strategies.
distinct_operators AS (
    SELECT *
    FROM (
        SELECT 
            *,
            -- We can use an arbitrary order here since the operator_tokens is the same for each (operator, strategy, hash, snapshot)
            -- We use strategy ASC for better debuggability
            ROW_NUMBER() OVER (
                PARTITION BY reward_hash, snapshot, operator 
                ORDER BY strategy ASC
            ) AS rn
        FROM reward_snapshot_operators
    ) t
    -- Keep only the first row for each (operator, reward hash, snapshot)
    WHERE rn = 1
),

-- Step 3: Calculate the tokens for each operator with dynamic split logic
-- If no split is found, default to 1000 (10%)
operator_splits AS (
    SELECT 
        dop.*,
        COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS DECIMAL) AS split_pct,
        FLOOR(dop.tokens_per_registered_snapshot_decimal * COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS DECIMAL)) AS operator_tokens
    FROM distinct_operators dop
    LEFT JOIN operator_set_split_snapshots oss
        ON dop.operator = oss.operator 
       AND dop.avs = oss.avs
       AND dop.operator_set_id = oss.operator_set_id 
       AND dop.snapshot = oss.snapshot
    LEFT JOIN default_operator_split_snapshots dos ON (dop.snapshot = dos.snapshot)
)

-- Step 4: Output the final table with operator splits
SELECT *, {{.generatedRewardsSnapshotId}} as generated_rewards_snapshot_id FROM operator_splits
`

func (rc *RewardsCalculator) GenerateGold12OperatorODOperatorSetRewardAmountsTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	rewardsV2_1Enabled, err := rc.globalConfig.IsRewardsV2_1EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}
	if !rewardsV2_1Enabled {
		rc.logger.Sugar().Infow("Rewards v2.1 is not enabled for this cutoff date, skipping GenerateGold12OperatorODOperatorSetRewardAmountsTable")
		return nil
	}

	destTableName := rewardsUtils.RewardsTable_12_OperatorODOperatorSetRewardAmounts

	rc.logger.Sugar().Infow("Generating Operator OD operator set reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_12_goldOperatorODOperatorSetRewardAmountsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"activeODRewardsTable":       rewardsUtils.RewardsTable_11_ActiveODOperatorSetRewards,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	query = query + " ON CONFLICT (reward_hash, operator_set_id, operator, strategy, snapshot) DO NOTHING"

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_operator_od_operator_set_reward_amounts", "error", res.Error)
		return res.Error
	}
	return nil
}
