package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _15_goldOperatorODOperatorSetRewardAmountsV2_2Query = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Get the rows where operators have registered for the operator set AND have allocated unique stake
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

-- V2.2: Filter operators who have allocated unique stake to this operator set
-- This ensures only operators who have committed unique stake can receive operator rewards
operators_with_unique_stake AS (
    SELECT DISTINCT
        rso.*
    FROM reward_snapshot_operators rso
    JOIN {{.operatorAllocationSnapshotsTable}} oas
        ON rso.operator = oas.operator
        AND rso.avs = oas.avs
        AND rso.operator_set_id = oas.operator_set_id
        AND rso.snapshot = oas.snapshot
    WHERE oas.magnitude > 0  -- Only include operators with active unique stake allocations
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
        FROM operators_with_unique_stake
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
SELECT * FROM operator_splits
`

func (rc *RewardsCalculator) GenerateGold12OperatorODOperatorSetRewardAmountsV2_2Table(snapshotDate string) error {
	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_12_OperatorODOperatorSetRewardAmounts]

	rc.logger.Sugar().Infow("Generating v2.2 Operator OD operator set reward amounts with unique stake",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_15_goldOperatorODOperatorSetRewardAmountsV2_2Query, map[string]interface{}{
		"destTableName":                    destTableName,
		"activeODRewardsTable":             allTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards],
		"operatorAllocationSnapshotsTable": allTableNames[rewardsUtils.Table_OperatorAllocationSnapshots],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_operator_od_operator_set_reward_amounts v2.2", "error", res.Error)
		return res.Error
	}
	return nil
}
