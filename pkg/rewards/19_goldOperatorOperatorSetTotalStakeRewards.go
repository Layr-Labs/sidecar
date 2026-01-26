package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _19_goldOperatorOperatorSetTotalStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- ============================================================================
-- PART 1: OD Rewards Path (per-operator amounts from Table 11)
-- ============================================================================

-- OD Step 1: Get registered operators for the operator set
WITH od_registered_operators AS (
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

-- OD Step 2: Calculate weighted total stake per operator
od_operators_with_weight AS (
    SELECT
        rso.*,
        oss.shares as operator_total_shares,
        SUM(CAST(oss.shares AS NUMERIC(78,0)) * rso.multiplier) OVER (
            PARTITION BY rso.reward_hash, rso.snapshot, rso.operator
        ) as operator_total_weight
    FROM od_registered_operators rso
    JOIN {{.operatorShareSnapshotsTable}} oss
        ON rso.operator = oss.operator
        AND rso.strategy = oss.strategy
        AND rso.snapshot = oss.snapshot
    WHERE oss.shares > 0
),

-- OD Step 3: Dedupe across strategies and prepare for common processing
od_distinct_operators AS (
    SELECT
        reward_hash, snapshot, token, tokens_per_registered_snapshot_decimal,
        avs, operator_set_id, operator, strategy, multiplier,
        reward_submission_date, operator_total_shares, operator_total_weight,
        'od' as reward_source
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY reward_hash, snapshot, operator ORDER BY strategy ASC) AS rn
        FROM od_operators_with_weight
    ) t WHERE rn = 1
),

-- ============================================================================
-- PART 2: Stake Rewards Path (pool amounts from Table 15, needs pro-rata)
-- ============================================================================

-- Stake Step 1: Get registered operators for stake-based total stake rewards
stake_registered_operators AS (
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

-- Stake Step 2: Calculate weighted total stake per operator
stake_operators_with_weight AS (
    SELECT
        rso.*,
        oss.shares as operator_total_shares,
        SUM(CAST(oss.shares AS NUMERIC(78,0)) * rso.multiplier) OVER (
            PARTITION BY rso.reward_hash, rso.snapshot, rso.operator
        ) as operator_total_weight
    FROM stake_registered_operators rso
    JOIN {{.operatorShareSnapshotsTable}} oss
        ON rso.operator = oss.operator
        AND rso.strategy = oss.strategy
        AND rso.snapshot = oss.snapshot
    WHERE oss.shares > 0
),

-- Stake Step 3: Calculate total weight per (reward_hash, snapshot) for pro-rata
stake_total_weight AS (
    SELECT DISTINCT
        reward_hash,
        snapshot,
        SUM(operator_total_weight) OVER (PARTITION BY reward_hash, snapshot) as total_weight
    FROM (
        SELECT DISTINCT reward_hash, snapshot, operator, operator_total_weight
        FROM stake_operators_with_weight
    ) distinct_ops
),

-- Stake Step 4: Calculate pro-rata tokens per operator and prepare for common processing
stake_distinct_operators AS (
    SELECT
        pow.reward_hash, pow.snapshot, pow.token,
        FLOOR(pow.tokens_per_day_decimal * pow.operator_total_weight / ptw.total_weight) as tokens_per_registered_snapshot_decimal,
        pow.avs, pow.operator_set_id, pow.operator, pow.strategy, pow.multiplier,
        pow.reward_submission_date, pow.operator_total_shares, pow.operator_total_weight,
        'stake' as reward_source
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY reward_hash, snapshot, operator ORDER BY strategy ASC) AS rn
        FROM stake_operators_with_weight
    ) pow
    JOIN stake_total_weight ptw
        ON pow.reward_hash = ptw.reward_hash
        AND pow.snapshot = ptw.snapshot
    WHERE pow.rn = 1 AND ptw.total_weight > 0
),

-- ============================================================================
-- PART 3: Common Processing (splits only - no slashing for TotalStake)
-- ============================================================================

-- Combine both paths (now have identical schema)
combined_operators AS (
    SELECT * FROM od_distinct_operators
    UNION ALL
    SELECT * FROM stake_distinct_operators
),

-- Common Step 1: Calculate operator tokens with dynamic split logic
-- If no split is found, default to 1000 (10%)
operator_splits AS (
    SELECT
        cop.*,
        COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS NUMERIC) AS split_pct,
        FLOOR(cop.tokens_per_registered_snapshot_decimal * COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS NUMERIC)) AS operator_tokens
    FROM combined_operators cop
    LEFT JOIN operator_set_split_snapshots oss
        ON cop.operator = oss.operator
       AND cop.avs = oss.avs
       AND cop.operator_set_id = oss.operator_set_id
       AND cop.snapshot = oss.snapshot
    LEFT JOIN default_operator_split_snapshots dos ON (cop.snapshot = dos.snapshot)
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
		"activeODRewardsTable":        allTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards],
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
