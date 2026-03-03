package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _21_goldAvsOperatorSetTotalStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Calculate total tokens available per (reward_hash, snapshot, avs, operator_set_id)
-- Table 15 has pool-level data without operator column
WITH total_available_tokens AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        avs,
        operator_set_id,
        MAX(tokens_per_day_decimal) as total_tokens
    FROM {{.activeStakeRewardsTable}}
    WHERE reward_type = 'total_stake'
    GROUP BY reward_hash, snapshot, token, avs, operator_set_id
),

-- Step 2: Calculate total operator tokens distributed (summed across all operators)
operator_distributed_tokens AS (
    SELECT
        reward_hash,
        snapshot,
        avs,
        operator_set_id,
        COALESCE(SUM(operator_tokens), 0) as operator_distributed
    FROM {{.operatorRewardsTable}}
    GROUP BY reward_hash, snapshot, avs, operator_set_id
),

-- Step 3: Calculate total staker tokens distributed (summed across all operators)
staker_distributed_tokens AS (
    SELECT
        reward_hash,
        snapshot,
        avs,
        operator_set_id,
        COALESCE(SUM(staker_tokens), 0) as staker_distributed
    FROM {{.stakerRewardsTable}}
    GROUP BY reward_hash, snapshot, avs, operator_set_id
),

-- Step 4: Identify operator-sets where total distributed tokens (operator + staker) = 0, refund those tokens to AVS
snapshots_requiring_refund AS (
    SELECT
        tat.reward_hash,
        tat.snapshot,
        tat.token,
        tat.avs,
        tat.operator_set_id,
        tat.total_tokens as avs_tokens
    FROM total_available_tokens tat
    LEFT JOIN operator_distributed_tokens odt
        ON tat.reward_hash = odt.reward_hash
        AND tat.snapshot = odt.snapshot
        AND tat.avs = odt.avs
        AND tat.operator_set_id = odt.operator_set_id
    LEFT JOIN staker_distributed_tokens sdt
        ON tat.reward_hash = sdt.reward_hash
        AND tat.snapshot = sdt.snapshot
        AND tat.avs = sdt.avs
        AND tat.operator_set_id = sdt.operator_set_id
    WHERE COALESCE(odt.operator_distributed, 0) + COALESCE(sdt.staker_distributed, 0) = 0
)

SELECT * FROM snapshots_requiring_refund
`

func (rc *RewardsCalculator) GenerateGold21AvsOperatorSetTotalStakeRewardsTable(snapshotDate string) error {
	rewardsV2_2Enabled, err := rc.globalConfig.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.2 is enabled", "error", err)
		return err
	}
	if !rewardsV2_2Enabled {
		rc.logger.Sugar().Infow("Rewards v2.2 is not enabled, skipping v2.2 table 21")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_21_AvsOperatorSetTotalStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 AVS operator set reward refunds with total stake validation",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_21_goldAvsOperatorSetTotalStakeRewardsQuery, map[string]interface{}{
		"destTableName":           destTableName,
		"activeStakeRewardsTable": allTableNames[rewardsUtils.Table_15_ActiveUniqueAndTotalStakeRewards],
		"operatorRewardsTable":    allTableNames[rewardsUtils.Table_19_OperatorOperatorSetTotalStakeRewards],
		"stakerRewardsTable":      allTableNames[rewardsUtils.Table_20_StakerOperatorSetTotalStakeRewards],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_avs_operator_set_total_stake_rewards v2.2", "error", res.Error)
		return res.Error
	}
	return nil
}
