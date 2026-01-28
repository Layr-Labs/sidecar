package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _16_goldOperatorOperatorSetUniqueStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- ============================================================================
-- PART 1: OD Rewards Path (per-operator amounts from Table 11)
-- ============================================================================

-- OD Step 1: Get registered operators with deregistration queue
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
        ap.reward_submission_date,
        osor.snapshot as registration_snapshot,
        osor.slashable_until
    FROM {{.activeODRewardsTable}} ap
    JOIN operator_set_operator_registration_snapshots osor
        ON ap.avs = osor.avs
       AND ap.operator_set_id = osor.operator_set_id
       AND ap.operator = osor.operator
       AND ap.snapshot >= osor.snapshot
       AND (osor.slashable_until IS NULL OR ap.snapshot <= osor.slashable_until)
),

-- OD Step 2: Calculate allocated weight per operator
od_operators_with_weight AS (
    SELECT
        rso.*,
        SUM(
            oss.shares *
            oas.magnitude /
            oas.max_magnitude * rso.multiplier
        ) OVER (PARTITION BY rso.reward_hash, rso.snapshot, rso.operator) as operator_allocated_weight
    FROM od_registered_operators rso
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

-- OD Step 3: Dedupe across strategies and prepare for common processing
od_distinct_operators AS (
    SELECT
        reward_hash, snapshot, token, tokens_per_registered_snapshot_decimal,
        avs, operator_set_id, operator, strategy, multiplier,
        reward_submission_date, registration_snapshot, slashable_until,
        operator_allocated_weight,
        'od' as reward_source
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY reward_hash, snapshot, operator ORDER BY strategy ASC) AS rn
        FROM od_operators_with_weight
    ) t WHERE rn = 1
),

-- ============================================================================
-- PART 2: Stake Rewards Path (pool amounts from Table 15, needs pro-rata)
-- ============================================================================

-- Stake Step 1: Get registered operators (join to get all operators for the operator set)
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
        osor.operator,
        osor.snapshot as registration_snapshot,
        osor.slashable_until
    FROM {{.activeStakeRewardsTable}} ap
    JOIN operator_set_operator_registration_snapshots osor
        ON ap.avs = osor.avs
       AND ap.operator_set_id = osor.operator_set_id
       AND ap.snapshot >= osor.snapshot
       AND (osor.slashable_until IS NULL OR ap.snapshot <= osor.slashable_until)
    WHERE ap.reward_type = 'unique_stake'
),

-- Stake Step 2: Calculate allocated weight per operator
stake_operators_with_weight AS (
    SELECT
        rso.*,
        SUM(
            oss.shares *
            oas.magnitude /
            oas.max_magnitude * rso.multiplier
        ) OVER (PARTITION BY rso.reward_hash, rso.snapshot, rso.operator) as operator_allocated_weight
    FROM stake_registered_operators rso
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

-- Stake Step 3: Calculate total weight per (reward_hash, snapshot) for pro-rata
stake_total_weight AS (
    SELECT DISTINCT
        reward_hash,
        snapshot,
        SUM(operator_allocated_weight) OVER (PARTITION BY reward_hash, snapshot) as total_weight
    FROM (
        SELECT DISTINCT reward_hash, snapshot, operator, operator_allocated_weight
        FROM stake_operators_with_weight
    ) distinct_ops
),

-- Stake Step 4: Calculate pro-rata tokens per operator and prepare for common processing
stake_distinct_operators AS (
    SELECT
        pow.reward_hash, pow.snapshot, pow.token,
        FLOOR(pow.tokens_per_day_decimal * pow.operator_allocated_weight / ptw.total_weight) as tokens_per_registered_snapshot_decimal,
        pow.avs, pow.operator_set_id, pow.operator, pow.strategy, pow.multiplier,
        pow.reward_submission_date, pow.registration_snapshot, pow.slashable_until,
        pow.operator_allocated_weight,
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
-- PART 3: Common Processing (slashing, splits)
-- ============================================================================

-- Combine both paths (now have identical schema)
combined_operators AS (
    SELECT * FROM od_distinct_operators
    UNION ALL
    SELECT * FROM stake_distinct_operators
),

-- Common Step 1: Check deregistration queue status
operators_with_deregistration_status AS (
    SELECT
        *,
        CASE
            WHEN slashable_until IS NOT NULL
             AND snapshot > (slashable_until - INTERVAL '14 days')
             AND snapshot <= slashable_until
            THEN TRUE
            ELSE FALSE
        END as in_deregistration_queue
    FROM combined_operators
),

-- Common Step 2: Calculate cumulative slash multiplier during deregistration queue
-- Slashing only affects rewards during the 14-day deregistration period
-- GUARD: If wad_slashed >= 1e18 (100% slash), use a very large negative value for LN
operators_with_slash_multiplier AS (
    SELECT
        owds.*,
        COALESCE(
            EXP(SUM(
                CASE
                    WHEN COALESCE(so.wad_slashed, 0) >= CAST(1e18 AS NUMERIC) THEN -100
                    ELSE LN(1 - COALESCE(so.wad_slashed, 0) / CAST(1e18 AS NUMERIC))
                END
            ) FILTER (
                WHERE owds.in_deregistration_queue
                  AND so.block_number > b_reg.number
                  AND so.block_number <= b_snapshot.number
            )),
            1.0
        ) as slash_multiplier
    FROM operators_with_deregistration_status owds
    LEFT JOIN slashed_operators so
        ON owds.operator = so.operator
       AND owds.avs = so.avs
       AND owds.operator_set_id = so.operator_set_id
       AND owds.strategy = so.strategy
    LEFT JOIN blocks b_reg
        ON DATE(b_reg.block_time) = owds.registration_snapshot
    LEFT JOIN blocks b_snapshot
        ON DATE(b_snapshot.block_time) = owds.snapshot
    GROUP BY owds.reward_hash, owds.snapshot, owds.token, owds.tokens_per_registered_snapshot_decimal,
             owds.avs, owds.operator_set_id, owds.operator, owds.strategy, owds.multiplier,
             owds.reward_submission_date, owds.registration_snapshot, owds.slashable_until,
             owds.operator_allocated_weight, owds.reward_source, owds.in_deregistration_queue
),

-- Common Step 3: Apply slash multiplier to tokens
operators_with_adjusted_tokens AS (
    SELECT
        *,
        CASE
            WHEN in_deregistration_queue THEN
                tokens_per_registered_snapshot_decimal * slash_multiplier
            ELSE
                tokens_per_registered_snapshot_decimal
        END as adjusted_tokens_per_snapshot
    FROM operators_with_slash_multiplier
),

-- Common Step 4: Calculate operator tokens with dynamic split logic
-- If no split is found, default to 1000 (10%)
operator_splits AS (
    SELECT
        oat.*,
        COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS NUMERIC) AS split_pct,
        FLOOR(oat.adjusted_tokens_per_snapshot * COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS NUMERIC)) AS operator_tokens
    FROM operators_with_adjusted_tokens oat
    LEFT JOIN operator_set_split_snapshots oss
        ON oat.operator = oss.operator
       AND oat.avs = oss.avs
       AND oat.operator_set_id = oss.operator_set_id
       AND oat.snapshot = oss.snapshot
    LEFT JOIN default_operator_split_snapshots dos ON (oat.snapshot = dos.snapshot)
)

SELECT * FROM operator_splits
`

func (rc *RewardsCalculator) GenerateGold16OperatorOperatorSetUniqueStakeRewardsTable(snapshotDate string) error {
	rewardsV2_2Enabled, err := rc.globalConfig.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.2 is enabled", "error", err)
		return err
	}
	if !rewardsV2_2Enabled {
		rc.logger.Sugar().Infow("Rewards v2.2 is not enabled, skipping v2.2 table 16")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_16_OperatorOperatorSetUniqueStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 Operator operator set unique stake rewards",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_16_goldOperatorOperatorSetUniqueStakeRewardsQuery, map[string]interface{}{
		"destTableName":                    destTableName,
		"activeODRewardsTable":             allTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards],
		"activeStakeRewardsTable":          allTableNames[rewardsUtils.Table_15_ActiveUniqueAndTotalStakeRewards],
		"operatorAllocationSnapshotsTable": "operator_allocation_snapshots",
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
