package rewards

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _17_goldAvsOperatorSetUniqueStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

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
        ap.multiplier
    FROM {{.activeODRewardsTable}} ap
    WHERE
        ap.num_registered_snapshots = 0
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

-- Step 4: Get operators who ARE registered but have NO allocated stake with magnitude > 0
-- This combines the previous scenarios of "no unique stake" and "strategies not registered"
-- If the operator doesn't have allocated stake with magnitude > 0, they get no rewards, so refund
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
        ap.multiplier
    FROM {{.activeODRewardsTable}} ap
    JOIN operator_set_operator_registration_snapshots osor
        ON ap.avs = osor.avs
        AND ap.operator_set_id = osor.operator_set_id
        AND ap.snapshot = osor.snapshot
        AND ap.operator = osor.operator
    WHERE ap.num_registered_snapshots != 0
),

-- Step 5: Find operators without allocated stake (magnitude > 0)
operators_without_allocated_stake AS (
    SELECT
        ro.*
    FROM registered_operators ro
    WHERE NOT EXISTS (
        SELECT 1
        FROM {{.operatorAllocationSnapshotsTable}} oas
        WHERE oas.operator = ro.operator
            AND oas.avs = ro.avs
            AND oas.operator_set_id = ro.operator_set_id
            AND oas.snapshot = ro.snapshot
            AND oas.magnitude > 0
    )
),

-- Step 6: Dedupe and calculate refunds for operators without allocated stake
distinct_operators_without_allocated_stake AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY reward_hash, snapshot, operator
                ORDER BY strategy ASC
            ) AS rn
        FROM operators_without_allocated_stake
    ) t
    WHERE rn = 1
),

-- Step 7: Calculate total refund for operators without allocated stake
avs_no_allocated_stake_refund_sums AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        avs,
        operator_set_id,
        operator,
        SUM(tokens_per_registered_snapshot_decimal) OVER (PARTITION BY reward_hash, snapshot) AS avs_tokens
    FROM distinct_operators_without_allocated_stake
),

-- Step 8: Combine all refund cases into one result
combined_avs_refund_amounts AS (
    SELECT * FROM avs_operator_refund_sums
    UNION ALL
    SELECT * FROM avs_no_allocated_stake_refund_sums
)

-- Output the final table
SELECT * FROM combined_avs_refund_amounts
`

func (rc *RewardsCalculator) GenerateGold17AvsOperatorSetUniqueStakeRewardsTable(snapshotDate string, forks config.ForkMap) error {
	rewardsV2_2Enabled, err := rc.globalConfig.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.2 is enabled", "error", err)
		return err
	}
	if !rewardsV2_2Enabled {
		rc.logger.Sugar().Infow("Rewards v2.2 is not enabled, skipping v2.2 table 17")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_17_AvsOperatorSetUniqueStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 AVS operator set unique stake rewards (refunds)",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
		zap.String("coloradoHardforkDate", forks[config.RewardsFork_Colorado].Date),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_17_goldAvsOperatorSetUniqueStakeRewardsQuery, map[string]interface{}{
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
		rc.logger.Sugar().Errorw("Failed to create gold_avs_operator_set_unique_stake_rewards v2.2", "error", res.Error)
		return res.Error
	}
	return nil
}
