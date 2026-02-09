package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _20_goldStakerOperatorSetTotalStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Get operator rewards and staker splits from previous table 19
WITH operator_rewards AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        operator,
        avs,
        operator_set_id,
        tokens_per_registered_snapshot_decimal,
        -- Calculate staker split (total rewards minus operator split)
        tokens_per_registered_snapshot_decimal - operator_tokens as staker_split_total
    FROM {{.operatorRewardsTable}}
),

-- Step 2: Get stakers delegated to each operator
staker_delegations AS (
    SELECT
        opr.*,
        sds.staker
    FROM operator_rewards opr
    JOIN staker_delegation_snapshots sds
        ON opr.operator = sds.operator
        AND opr.snapshot = sds.snapshot
),

-- Step 3: Get each staker's weighted shares across strategies in the reward submission
staker_strategy_shares AS (
    SELECT
        sd.reward_hash,
        sd.snapshot,
        sd.token,
        sd.operator,
        sd.avs,
        sd.operator_set_id,
        sd.staker,
        sd.tokens_per_registered_snapshot_decimal,
        sd.staker_split_total,
        SUM(sss.shares * asr.multiplier) as weighted_shares
    FROM staker_delegations sd
    JOIN {{.activeStakeRewardsTable}} asr
        ON sd.reward_hash = asr.reward_hash
        AND sd.avs = asr.avs
        AND sd.operator_set_id = asr.operator_set_id
        AND sd.snapshot = asr.snapshot
    JOIN staker_share_snapshots sss
        ON sd.staker = sss.staker
        AND asr.strategy = sss.strategy
        AND sd.snapshot = sss.snapshot
    WHERE sss.shares > 0
        AND asr.multiplier != 0
    GROUP BY sd.reward_hash, sd.snapshot, sd.token, sd.operator,
             sd.avs, sd.operator_set_id, sd.staker,
             sd.tokens_per_registered_snapshot_decimal, sd.staker_split_total
),

-- Step 4: Calculate each staker's weight
staker_weights AS (
    SELECT
        *,
        weighted_shares as staker_weight
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
	activeStakeRewardsTable := allTableNames[rewardsUtils.Table_15_ActiveUniqueAndTotalStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 staker operator set reward amounts with total stake",
		zap.String("snapshotDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_20_goldStakerOperatorSetTotalStakeRewardsQuery, map[string]interface{}{
		"destTableName":           destTableName,
		"operatorRewardsTable":    operatorRewardsTable,
		"activeStakeRewardsTable": activeStakeRewardsTable,
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
