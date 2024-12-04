package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _8_goldOperatorODRewardAmountsQuery = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Get the rows where operators have registered for the AVS
WITH reward_snapshot_operators AS (
    SELECT
        ap.reward_hash,
        ap.snapshot AS snapshot,
        ap.token,
        ap.tokens_per_day,
        ap.tokens_per_day_decimal,
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
        COALESCE(oas.split, 1000) / CAST(10000 AS DECIMAL) as split_pct,
        FLOOR(dop.tokens_per_day_decimal * COALESCE(oas.split, 1000) / CAST(10000 AS DECIMAL)) AS operator_tokens
    FROM distinct_operators dop
    LEFT JOIN operator_avs_split_snapshots oas
        ON dop.operator = oas.operator 
       AND dop.avs = oas.avs 
       AND dop.snapshot = oas.snapshot
)

-- Step 4: Output the final table with operator splits
SELECT * FROM operator_splits
`

func (rc *RewardsCalculator) GenerateGold8OperatorODRewardAmountsTable(snapshotDate string) error {
	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_8_OperatorODRewardAmounts]

	rc.logger.Sugar().Infow("Generating Operator OD reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_8_goldOperatorODRewardAmountsQuery, map[string]string{
		"destTableName":        destTableName,
		"activeODRewardsTable": allTableNames[rewardsUtils.Table_7_ActiveODRewards],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_operator_od_reward_amounts", "error", res.Error)
		return res.Error
	}
	return nil
}
