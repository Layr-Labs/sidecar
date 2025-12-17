package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _16_goldStakerOperatorSetUniqueStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Get operator rewards and staker splits from previous table 15
WITH operator_rewards AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        operator,
        avs,
        operator_set_id,
        strategy,
        tokens_per_registered_snapshot_decimal,
        -- Calculate staker split (total rewards minus operator split)
        tokens_per_registered_snapshot_decimal - operator_tokens as staker_split_total
    FROM {{.operatorRewardsTable}}
),

-- Step 2: Get stakers delegated to each operator
staker_delegations AS (
    SELECT
        or.*,
        sds.staker
    FROM operator_rewards or
    JOIN staker_delegation_snapshots sds
        ON or.operator = sds.operator
        AND or.snapshot = sds.snapshot
),

-- Step 3: Get each staker's shares for the strategy
staker_strategy_shares AS (
    SELECT
        sd.*,
        sss.shares,
        sd.multiplier
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
        shares * multiplier as staker_weight
    FROM staker_strategy_shares
),

-- Step 5: Calculate total weight per operator
staker_weight_with_totals AS (
    SELECT
        *,
        SUM(staker_weight) OVER (PARTITION BY reward_hash, operator, snapshot) as total_operator_weight
    FROM staker_weights
),

-- Step 6: Calculate staker proportions with 15 decimal place precision
staker_proportions AS (
    SELECT
        *,
        CASE
            WHEN total_operator_weight > 0 THEN
                FLOOR((staker_weight / total_operator_weight) * 1000000000000000) / 1000000000000000
            ELSE 0
        END as staker_proportion
    FROM staker_weight_with_totals
),

-- Step 7: Calculate staker rewards
staker_rewards AS (
    SELECT
        *,
        CASE
            WHEN total_operator_weight > 0 THEN
                FLOOR(staker_proportion * staker_split_total)
            ELSE 0
        END as staker_tokens
    FROM staker_proportions
)

-- Output the final table
SELECT * FROM staker_rewards
`

func (rc *RewardsCalculator) GenerateGold16StakerOperatorSetUniqueStakeRewardsTable(snapshotDate string) error {
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
	destTableName := allTableNames[rewardsUtils.Table_16_StakerOperatorSetUniqueStakeRewards]
	operatorRewardsTable := allTableNames[rewardsUtils.Table_15_OperatorOperatorSetUniqueStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 staker operator set unique stake rewards",
		zap.String("snapshotDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_16_goldStakerOperatorSetUniqueStakeRewardsQuery, map[string]interface{}{
		"destTableName":        destTableName,
		"operatorRewardsTable": operatorRewardsTable,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render v2.2 query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to generate v2.2 staker operator set unique stake rewards", "error", res.Error)
		return res.Error
	}

	rc.logger.Sugar().Infow("Successfully generated v2.2 unique stake rewards",
		zap.String("snapshotDate", snapshotDate),
		zap.Int64("rowsAffected", res.RowsAffected),
	)

	return nil
}
