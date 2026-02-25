package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _19_goldOperatorOperatorSetTotalStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- ============================================================================
-- Stake Rewards Path (pool amounts from Table 15, needs pro-rata)
-- ============================================================================

-- Step 1: Get registered operators for stake-based total stake rewards
WITH registered_operators AS (
    SELECT
        ap.reward_hash,
        ap.snapshot AS snapshot,
        ap.token,
        ap.tokens_per_day_decimal,
        ap.avs AS avs,
        ap.operator_set_id AS operator_set_id,
        ap.strategy,
        ap.multiplier,
        ap.reward_submission_date,
        osor.operator
    FROM {{.activeStakeRewardsTable}} ap
    JOIN operator_set_operator_registration_snapshots osor
        ON ap.avs = osor.avs
       AND ap.operator_set_id = osor.operator_set_id
       AND ap.snapshot = osor.snapshot
    WHERE ap.reward_type = 'total_stake'
),

-- Step 2: Calculate weighted total stake per operator
operators_with_weight AS (
    SELECT
        rso.*,
        oss.shares as operator_total_shares,
        SUM(oss.shares * rso.multiplier) OVER (
            PARTITION BY rso.reward_hash, rso.snapshot, rso.operator
        ) as operator_total_weight
    FROM registered_operators rso
    JOIN {{.operatorShareSnapshotsTable}} oss
        ON rso.operator = oss.operator
        AND rso.strategy = oss.strategy
        AND rso.snapshot = oss.snapshot
    WHERE oss.shares > 0
),

-- Step 3: Calculate total weight per (reward_hash, snapshot) for pro-rata
total_weight AS (
    SELECT DISTINCT
        reward_hash,
        snapshot,
        SUM(operator_total_weight) OVER (PARTITION BY reward_hash, snapshot) as total_weight
    FROM (
        SELECT DISTINCT reward_hash, snapshot, operator, operator_total_weight
        FROM operators_with_weight
    ) distinct_ops
),

-- Step 4: Calculate pro-rata tokens per operator
distinct_operators AS (
    SELECT
        pow.reward_hash, pow.snapshot, pow.token,
        FLOOR(pow.tokens_per_day_decimal * pow.operator_total_weight / tw.total_weight) as tokens_per_registered_snapshot_decimal,
        pow.avs, pow.operator_set_id, pow.operator, pow.strategy, pow.multiplier,
        pow.reward_submission_date, pow.operator_total_shares, pow.operator_total_weight
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY reward_hash, snapshot, operator ORDER BY CASE WHEN multiplier = 0 THEN 1 ELSE 0 END, strategy ASC) AS rn
        FROM operators_with_weight
    ) pow
    JOIN total_weight tw
        ON pow.reward_hash = tw.reward_hash
        AND pow.snapshot = tw.snapshot
    WHERE pow.rn = 1 AND tw.total_weight > 0
),

-- ============================================================================
-- Splits Processing (no slashing for TotalStake)
-- ============================================================================

-- Step 5: Calculate operator tokens with dynamic split logic
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

SELECT * FROM operator_splits
`

func (rc *RewardsCalculator) GenerateGold19OperatorOperatorSetTotalStakeRewardsTable(snapshotDate string) error {
	rewardsV2_2Enabled, err := rc.globalConfig.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.2 is enabled", "error", err)
		return err
	}
	if !rewardsV2_2Enabled {
		rc.logger.Sugar().Infow("Rewards v2.2 is not enabled, skipping v2.2 table 19")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_19_OperatorOperatorSetTotalStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 Operator operator set reward amounts with total stake",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_19_goldOperatorOperatorSetTotalStakeRewardsQuery, map[string]interface{}{
		"destTableName":               destTableName,
		"activeStakeRewardsTable":     allTableNames[rewardsUtils.Table_15_ActiveUniqueAndTotalStakeRewards],
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
