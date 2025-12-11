package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _15_goldOperatorOperatorSetUniqueStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Get registered operators for the operator set
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

operators_with_allocated_stake AS (
    SELECT
        rso.*,
        oas.magnitude,
        oas.max_magnitude,
        oss.shares as operator_total_shares,
        -- Calculate effective allocated stake for this strategy
        CAST(oss.shares AS NUMERIC(78,0)) *
        CAST(oas.magnitude AS NUMERIC(78,0)) /
        CAST(oas.max_magnitude AS NUMERIC(78,0)) as allocated_stake
    FROM reward_snapshot_operators rso
    JOIN {{.operatorAllocationSnapshotsTable}} oas
        ON rso.operator = oas.operator
        AND rso.avs = oas.avs
        AND rso.strategy = oas.strategy
        AND rso.operator_set_id = oas.operator_set_id
        AND rso.snapshot = oas.snapshot
    JOIN {{.operatorShareSnapshotsTable}} oss
        ON rso.operator = oss.operator
        AND rso.strategy = oss.strategy
        AND rso.snapshot = oss.snapshot
    WHERE oas.magnitude > 0
        AND oas.max_magnitude > 0
        AND oss.shares > 0
),

-- Calculate weighted allocated stake per operator (sum across strategies)
operators_with_unique_stake AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        tokens_per_registered_snapshot_decimal,
        avs,
        operator_set_id,
        operator,
        strategy,
        multiplier,
        reward_submission_date,
        -- Sum the weighted allocated stake across strategies
        SUM(allocated_stake * multiplier) OVER (
            PARTITION BY reward_hash, snapshot, operator
        ) as operator_allocated_weight
    FROM operators_with_allocated_stake
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
        COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS NUMERIC) AS split_pct,
        FLOOR(dop.tokens_per_registered_snapshot_decimal * COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS NUMERIC)) AS operator_tokens
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

func (rc *RewardsCalculator) GenerateGold15OperatorOperatorSetUniqueStakeRewardsTable(snapshotDate string) error {
	rewardsV2_2Enabled, err := rc.globalConfig.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.2 is enabled", "error", err)
		return err
	}
	if !rewardsV2_2Enabled {
		rc.logger.Sugar().Infow("Rewards v2.2 is not enabled, skipping v2.2 table 15")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_15_OperatorOperatorSetUniqueStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 Operator operator set unique stake rewards",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_15_goldOperatorOperatorSetUniqueStakeRewardsQuery, map[string]interface{}{
		"destTableName":                    destTableName,
		"activeODRewardsTable":             allTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards],
		"operatorAllocationSnapshotsTable": allTableNames[rewardsUtils.Table_OperatorAllocationSnapshots],
		"operatorShareSnapshotsTable":      "operator_share_snapshots",
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_operator_operator_set_unique_stake_rewards v2.2", "error", res.Error)
		return res.Error
	}
	return nil
}
