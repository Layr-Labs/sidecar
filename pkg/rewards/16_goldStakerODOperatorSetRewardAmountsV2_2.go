package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _16_goldStakerODOperatorSetRewardAmountsV2_2Query = `
CREATE TABLE {{.destTableName}} AS

-- V2.2: Operator Set Rewards with UNIQUE STAKE instead of total stake
-- CRITICAL: Unique stake MUST be allocated to the SPECIFIC operator set for rewards to be distributed

-- Step 1: Get the rows where operators have registered for the operator set
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

-- Get the rows where strategies have registered for the operator set
operator_set_strategy_registrations AS (
    SELECT
        rso.*
    FROM reward_snapshot_operators rso
    JOIN operator_set_strategy_registration_snapshots ossr
        ON rso.avs = ossr.avs 
       AND rso.operator_set_id = ossr.operator_set_id
       AND rso.snapshot = ossr.snapshot
       AND rso.strategy = ossr.strategy
),

-- V2.2 CRITICAL: Get unique stake allocations for this SPECIFIC operator set
-- This ensures rewards are only distributed based on stake that is:
-- 1. Allocated to THIS specific operator set (not other operator sets)
-- 2. Slashable (magnitude > 0)
-- 3. Effective at the snapshot date (handles retroactive rewards)
operator_unique_stake_allocations AS (
    SELECT
        ossr.*,
        oas.magnitude as unique_stake_magnitude
    FROM operator_set_strategy_registrations ossr
    JOIN {{.operatorAllocationSnapshotsTable}} oas
        ON ossr.operator = oas.operator
       AND ossr.avs = oas.avs
       AND ossr.strategy = oas.strategy
       AND ossr.snapshot = oas.snapshot
       -- CRITICAL: Must match the SPECIFIC operator set
       AND ossr.operator_set_id = oas.operator_set_id
    WHERE oas.magnitude > 0  -- Only slashable (allocated) unique stake
),

-- Calculate the total staker split for each operator reward with dynamic split logic
staker_splits AS (
    SELECT 
        ousa.*,
        ousa.tokens_per_registered_snapshot_decimal - FLOOR(ousa.tokens_per_registered_snapshot_decimal * COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS DECIMAL)) AS staker_split
    FROM operator_unique_stake_allocations ousa
    LEFT JOIN operator_set_split_snapshots oss
        ON ousa.operator = oss.operator 
       AND ousa.avs = oss.avs 
       AND ousa.operator_set_id = oss.operator_set_id
       AND ousa.snapshot = oss.snapshot
    LEFT JOIN default_operator_split_snapshots dos ON (ousa.snapshot = dos.snapshot)
),

-- Get the stakers that were delegated to the operator for the snapshot
staker_delegated_operators AS (
    SELECT
        ss.*,
        sds.staker
    FROM staker_splits ss
    JOIN staker_delegation_snapshots sds
        ON ss.operator = sds.operator 
       AND ss.snapshot = sds.snapshot
),

-- V2.2: Calculate staker proportions based on their delegated shares
-- BUT limit the reward pool to the unique stake allocated to this operator set
staker_strategy_shares AS (
    SELECT
        sdo.*,
        sss.shares
    FROM staker_delegated_operators sdo
    JOIN staker_share_snapshots sss
        ON sdo.staker = sss.staker 
       AND sdo.snapshot = sss.snapshot 
       AND sdo.strategy = sss.strategy
    WHERE sss.shares > 0 AND sdo.multiplier != 0
),

-- Calculate the weight of each staker based on their total delegated shares
staker_weights AS (
    SELECT 
        *,
        SUM(multiplier * shares) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
    FROM staker_strategy_shares
),

-- Get distinct stakers
distinct_stakers AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY reward_hash, snapshot, staker 
                ORDER BY strategy ASC
            ) AS rn
        FROM staker_weights
    ) t
    WHERE rn = 1
    ORDER BY reward_hash, snapshot, staker
),

-- V2.2: Calculate total weight and unique stake for the operator set
staker_weight_and_unique_stake AS (
    SELECT 
        *,
        SUM(staker_weight) OVER (PARTITION BY reward_hash, operator, snapshot) AS total_weight,
        -- V2.2: Use UNIQUE stake magnitude as the cap for rewards
        MAX(CAST(unique_stake_magnitude AS DECIMAL(38,0))) OVER (PARTITION BY reward_hash, operator, snapshot) AS total_unique_stake_for_operator_set
    FROM distinct_stakers
),

-- Calculate staker proportion based on their weight
staker_proportion AS (
    SELECT 
        *,
        FLOOR((staker_weight / total_weight) * 1000000000000000) / 1000000000000000 AS staker_proportion
    FROM staker_weight_and_unique_stake
),

-- V2.2: Calculate rewards based on staker proportion of the UNIQUE stake allocation
-- NOT based on total delegated stake
staker_reward_amounts AS (
    SELECT 
        *,
        CASE
            -- V2.2: Staker gets their proportion of the staker split
            -- But the split is calculated from the unique stake allocated to this operator set
            -- This ensures rewards can never exceed the unique stake allocation
            WHEN total_unique_stake_for_operator_set > 0 THEN
                FLOOR(staker_proportion * staker_split)
            ELSE
                0  -- No unique stake allocated = no rewards
        END AS staker_tokens
    FROM staker_proportion
)

-- Output the final table
SELECT * FROM staker_reward_amounts
`

func (rc *RewardsCalculator) GenerateGold13StakerODOperatorSetRewardAmountsV2_2Table(snapshotDate string) error {
	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_13_StakerODOperatorSetRewardAmounts]
	activeODRewardsTable := allTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards]
	operatorAllocationSnapshotsTable := allTableNames[rewardsUtils.Table_OperatorAllocationSnapshots]

	rc.logger.Sugar().Infow("Generating v2.2 staker OD operator set reward amounts with unique stake",
		zap.String("snapshotDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_16_goldStakerODOperatorSetRewardAmountsV2_2Query, map[string]interface{}{
		"destTableName":                    destTableName,
		"activeODRewardsTable":             activeODRewardsTable,
		"operatorAllocationSnapshotsTable": operatorAllocationSnapshotsTable,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render v2.2 query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to generate v2.2 staker OD operator set reward amounts", "error", res.Error)
		return res.Error
	}

	rc.logger.Sugar().Infow("Successfully generated v2.2 unique stake rewards",
		zap.String("snapshotDate", snapshotDate),
		zap.Int64("rowsAffected", res.RowsAffected),
	)

	return nil
}
