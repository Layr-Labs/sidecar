package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _15_goldOperatorOperatorSetUniqueStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Get registered operators for the operator set
-- Uses slashable_until to include 14-day deregistration queue for unique stake rewards
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
        ap.reward_submission_date,
        osor.snapshot as registration_snapshot,
        osor.slashable_until
    FROM {{.activeODRewardsTable}} ap
    JOIN operator_set_operator_registration_snapshots osor
        ON ap.avs = osor.avs
       AND ap.operator_set_id = osor.operator_set_id
       AND ap.operator = osor.operator
       -- Use slashable_until for unique stake rewards (includes 14-day queue)
       -- NULL slashable_until means operator is still active
       AND ap.snapshot >= osor.snapshot
       AND (osor.slashable_until IS NULL OR ap.snapshot <= osor.slashable_until)
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
        registration_snapshot,
        slashable_until,
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

-- Step 3: Check if operators are in deregistration queue (14-day window after deregistration)
operators_with_deregistration_status AS (
    SELECT
        dop.*,
        CASE
            WHEN dop.slashable_until IS NOT NULL
             AND dop.snapshot > (dop.slashable_until - INTERVAL '14 days')
             AND dop.snapshot <= dop.slashable_until
            THEN TRUE
            ELSE FALSE
        END as in_deregistration_queue
    FROM distinct_operators dop
),

-- Step 4: Calculate cumulative slash multiplier during deregistration queue
-- Slashing only affects rewards during the 14-day deregistration period
-- GUARD: If wad_slashed >= 1e18 (100% slash), use a very large negative value for LN
-- instead of LN(0) which would cause a math error. This effectively makes the multiplier ~0.
operators_with_slash_multiplier AS (
    SELECT
        owds.*,
        COALESCE(
            EXP(SUM(
                CASE
                    WHEN COALESCE(so.wad_slashed, 0) >= CAST(1e18 AS NUMERIC) THEN -100  -- Effectively 0 multiplier (e^-100 ≈ 0)
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
             owds.operator_allocated_weight, owds.rn, owds.in_deregistration_queue
),

-- Step 5: Apply slash multiplier to tokens
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

-- Step 6: Calculate the tokens for each operator with dynamic split logic
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

-- Step 7: Output the final table with operator splits
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
