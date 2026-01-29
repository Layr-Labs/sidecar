package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _20_goldStakerOperatorSetTotalStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Get operator rewards and staker splits from previous table 18
WITH operator_rewards AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        operator,
        avs,
        operator_set_id,
        strategy,
        multiplier,
        tokens_per_registered_snapshot_decimal,
        -- Calculate staker split (total rewards minus operator split)
        tokens_per_registered_snapshot_decimal - operator_tokens as staker_split_total
    FROM {{.operatorRewardsTable}}
),

-- Step 2: Get stakers delegated to each operator
staker_delegations AS (
    SELECT
        op_rew.*,
        sds.staker
    FROM operator_rewards op_rew
    JOIN staker_delegation_snapshots sds
        ON op_rew.operator = sds.operator
        AND op_rew.snapshot = sds.snapshot
),

-- Step 3: Get each staker's shares for the strategy
staker_strategy_shares AS (
    SELECT
        sd.*,
        sss.shares
    FROM staker_delegations sd
    JOIN staker_share_snapshots sss
        ON sd.staker = sss.staker
        AND sd.strategy = sss.strategy
        AND sd.snapshot = sss.snapshot
    WHERE sss.shares > 0
        AND sd.multiplier != 0
),

-- Step 4: Calculate each staker's weighted shares
staker_weights AS (
    SELECT
        *,
        CAST(shares AS NUMERIC(78,0)) * multiplier as staker_weight
    FROM staker_strategy_shares
),

-- Step 5: Calculate total weight per operator
staker_weight_with_totals AS (
    SELECT
        *,
        SUM(staker_weight) OVER (PARTITION BY reward_hash, operator, snapshot) as total_operator_weight
    FROM staker_weights
),

-- Step 6: Calculate staker proportions and rewards
staker_rewards AS (
    SELECT
        *,
        -- Staker's proportion of operator's total delegated stake
        CASE
            WHEN total_operator_weight > 0 THEN
                staker_weight / total_operator_weight
            ELSE 0
        END as staker_proportion,
        -- Staker's reward = proportion * operator's staker_split_total
        CASE
            WHEN total_operator_weight > 0 THEN
                FLOOR((staker_weight / total_operator_weight) * staker_split_total)
            ELSE 0
        END as staker_tokens
    FROM staker_weight_with_totals
)

-- Output the final table
SELECT * FROM staker_rewards
`

func (rc *RewardsCalculator) GenerateGold20StakerOperatorSetTotalStakeRewardsTable(snapshotDate string) error {
	rewardsV2_2Enabled, err := rc.globalConfig.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.2 is enabled", "error", err)
		return err
	}
	if !rewardsV2_2Enabled {
		rc.logger.Sugar().Infow("Rewards v2.2 is not enabled, skipping v2.2 table 20")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_20_StakerOperatorSetTotalStakeRewards]
	operatorRewardsTable := allTableNames[rewardsUtils.Table_19_OperatorOperatorSetTotalStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 staker operator set reward amounts with total stake",
		zap.String("snapshotDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_20_goldStakerOperatorSetTotalStakeRewardsQuery, map[string]interface{}{
		"destTableName":        destTableName,
		"operatorRewardsTable": operatorRewardsTable,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render v2.2 query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to generate v2.2 staker operator set total stake rewards", "error", res.Error)
		return res.Error
	}

	rc.logger.Sugar().Infow("Successfully generated v2.2 total stake rewards",
		zap.String("snapshotDate", snapshotDate),
		zap.Int64("rowsAffected", res.RowsAffected),
	)

	return nil
}
