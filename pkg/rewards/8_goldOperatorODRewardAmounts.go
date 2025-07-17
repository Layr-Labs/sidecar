package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _8_goldOperatorODRewardAmountsQuery = `
INSERT INTO {{.destTableName}} (reward_hash, snapshot, token, tokens_per_registered_snapshot_decimal, avs, operator, strategy, multiplier, reward_submission_date, rn, split_pct, operator_tokens, generated_rewards_snapshot_id)

-- Step 1: Get the rows where operators have registered for the AVS
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
    JOIN operator_avs_registration_snapshots oar
        ON ap.avs = oar.avs 
       AND ap.snapshot = oar.snapshot 
       AND ap.operator = oar.operator
),

-- Step 2: Dedupe the operator tokens across strategies for each (operator, reward hash, snapshot)
-- Since the above result is a flattened operator-directed reward submission across strategies.
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
    -- Keep only the first row for each (operator, reward hash, snapshot)
    WHERE rn = 1
),

-- Step 3: Calculate the tokens for each operator with dynamic split logic
-- If no split is found, default to 1000 (10%)
operator_splits AS (
    SELECT 
        dop.*,
        CASE
            WHEN dop.snapshot < @trinityHardforkDate AND dop.reward_submission_date < @trinityHardforkDate THEN
                COALESCE(oas.split, 1000) / CAST(10000 AS DECIMAL)
            ELSE
                COALESCE(oas.split, dos.split, 1000) / CAST(10000 AS DECIMAL)
        END AS split_pct,
        CASE
            WHEN dop.snapshot < @trinityHardforkDate AND dop.reward_submission_date < @trinityHardforkDate THEN
                FLOOR(dop.tokens_per_registered_snapshot_decimal * COALESCE(oas.split, 1000) / CAST(10000 AS DECIMAL))
            ELSE
                FLOOR(dop.tokens_per_registered_snapshot_decimal * COALESCE(oas.split, dos.split, 1000) / CAST(10000 AS DECIMAL))
        END AS operator_tokens
    FROM distinct_operators dop
    LEFT JOIN operator_avs_split_snapshots oas
        ON dop.operator = oas.operator 
       AND dop.avs = oas.avs 
       AND dop.snapshot = oas.snapshot
    LEFT JOIN default_operator_split_snapshots dos ON (dop.snapshot = dos.snapshot)
)

-- Step 4: Output the final table with operator splits
SELECT *, {{.generatedRewardsSnapshotId}} as generated_rewards_snapshot_id FROM operator_splits
ON CONFLICT (reward_hash, avs, operator, strategy, snapshot) DO NOTHING
`

func (rc *RewardsCalculator) GenerateGold8OperatorODRewardAmountsTable(snapshotDate string, generatedRewardsSnapshotId uint64, forks config.ForkMap) error {
	rewardsV2Enabled, err := rc.globalConfig.IsRewardsV2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}
	if !rewardsV2Enabled {
		rc.logger.Sugar().Infow("Rewards v2 is not enabled for this cutoff date, skipping GenerateGold8OperatorODRewardAmountsTable")
		return nil
	}

	destTableName := rewardsUtils.RewardsTable_8_OperatorODRewardAmounts
	activeOdRewardsTableName := rc.getTempActiveODRewardsTableName(snapshotDate, generatedRewardsSnapshotId)

	rc.logger.Sugar().Infow("Generating Operator OD reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
		zap.String("trinityHardforkDate", forks[config.RewardsFork_Trinity].Date),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_8_goldOperatorODRewardAmountsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"activeODRewardsTable":       activeOdRewardsTableName,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query, sql.Named("trinityHardforkDate", forks[config.RewardsFork_Trinity].Date))
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_operator_od_reward_amounts", "error", res.Error)
		return res.Error
	}
	return nil
}
