package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _16_goldOperatorOperatorSetUniqueStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- ============================================================================
-- Stake Rewards Path (pool amounts from Table 15, needs pro-rata)
-- ============================================================================

-- Step 1: Get registered operators (join to get all operators for the operator set)
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

-- Step 2: Calculate allocated weight per operator
operators_with_weight AS (
    SELECT
        rso.*,
        SUM(
            oss.shares *
            oas.magnitude /
            oas.max_magnitude * rso.multiplier
        ) OVER (PARTITION BY rso.reward_hash, rso.snapshot, rso.operator) as operator_allocated_weight
    FROM registered_operators rso
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

-- Step 3: Calculate total weight per (reward_hash, snapshot) for pro-rata
total_weight AS (
    SELECT DISTINCT
        reward_hash,
        snapshot,
        SUM(operator_allocated_weight) OVER (PARTITION BY reward_hash, snapshot) as total_weight
    FROM (
        SELECT DISTINCT reward_hash, snapshot, operator, operator_allocated_weight
        FROM operators_with_weight
    ) distinct_ops
),

-- Step 4: Calculate pro-rata tokens per operator
distinct_operators AS (
    SELECT
        pow.reward_hash, pow.snapshot, pow.token,
        FLOOR(pow.tokens_per_day_decimal * pow.operator_allocated_weight / tw.total_weight) as tokens_per_registered_snapshot_decimal,
        pow.avs, pow.operator_set_id, pow.operator, pow.strategy, pow.multiplier,
        pow.reward_submission_date, pow.registration_snapshot, pow.slashable_until,
        pow.operator_allocated_weight
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
-- Slashing and Splits Processing
-- ============================================================================

-- Step 5: Check deregistration queue status
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
    FROM distinct_operators
),

-- Step 6: Calculate cumulative slash multiplier during deregistration queue
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
                  AND so.block_number >= b_reg.number
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
    LEFT JOIN LATERAL (
        SELECT number
        FROM blocks
        WHERE DATE(block_time) = owds.registration_snapshot
        ORDER BY number ASC
        LIMIT 1
    ) b_reg ON true
    LEFT JOIN LATERAL (
        SELECT number
        FROM blocks
        WHERE DATE(block_time) = owds.snapshot
        ORDER BY number DESC
        LIMIT 1
    ) b_snapshot ON true
    GROUP BY owds.reward_hash, owds.snapshot, owds.token, owds.tokens_per_registered_snapshot_decimal,
             owds.avs, owds.operator_set_id, owds.operator, owds.strategy, owds.multiplier,
             owds.reward_submission_date, owds.registration_snapshot, owds.slashable_until,
             owds.operator_allocated_weight, owds.in_deregistration_queue
),

-- Step 7: Apply slash multiplier to tokens
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

-- Step 8: Calculate operator tokens with dynamic split logic
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
