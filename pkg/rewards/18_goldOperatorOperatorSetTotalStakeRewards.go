package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _18_goldOperatorOperatorSetTotalStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- V2.2: Total Stake Weighted Rewards for Operator Sets
-- Key difference from Unique Stake: Uses total delegated stake instead of allocated unique stake
-- No allocation validation required - just operator registration to set

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

-- Step 2: Get operator's total delegated stake per strategy
operators_with_total_stake AS (
    SELECT
        rso.*,
        oss.shares as operator_total_shares
    FROM reward_snapshot_operators rso
    JOIN {{.operatorShareSnapshotsTable}} oss
        ON rso.operator = oss.operator
        AND rso.strategy = oss.strategy
        AND rso.snapshot = oss.snapshot
    WHERE oss.shares > 0
),

-- Step 3: Calculate weighted total stake per operator (sum across strategies with multiplier)
operators_with_total_stake_weight AS (
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
        operator_total_shares,
        -- Sum the weighted total stake across strategies
        SUM(CAST(operator_total_shares AS DECIMAL(78,0)) * multiplier) OVER (
            PARTITION BY reward_hash, snapshot, operator
        ) as operator_total_weight
    FROM operators_with_total_stake
),

-- Step 4: Dedupe across strategies (take first strategy, they all have same operator_total_weight)
distinct_operators AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY reward_hash, snapshot, operator
                ORDER BY strategy ASC
            ) AS rn
        FROM operators_with_total_stake_weight
    ) t
    WHERE rn = 1
),

-- Step 5: Calculate operator splits with dynamic split logic
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

-- Output the final table
SELECT * FROM operator_splits
`

func (rc *RewardsCalculator) GenerateGold18OperatorOperatorSetTotalStakeRewardsTable(snapshotDate string) error {
	// Skip if v2.2 is not enabled
	rewardsV2_2Enabled, err := rc.globalConfig.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.2 is enabled", "error", err)
		return err
	}
	if !rewardsV2_2Enabled {
		rc.logger.Sugar().Infow("Rewards v2.2 is not enabled, skipping v2.2 table 18")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_18_OperatorOperatorSetTotalStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 Operator operator set reward amounts with total stake",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_18_goldOperatorOperatorSetTotalStakeRewardsQuery, map[string]interface{}{
		"destTableName":               destTableName,
		"activeODRewardsTable":        allTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards],
		"operatorShareSnapshotsTable": "operator_share_snapshots",
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_operator_operator_set_total_stake_rewards v2.2", "error", res.Error)
		return res.Error
	}
	return nil
}
