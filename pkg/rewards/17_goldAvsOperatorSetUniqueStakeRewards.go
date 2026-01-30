package rewards

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _17_goldAvsOperatorSetUniqueStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Calculate total tokens available per (reward_hash, snapshot)
WITH total_available_tokens AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        avs,
        operator_set_id,
        operator,
        SUM(tokens_per_registered_snapshot_decimal) as total_tokens
    FROM {{.activeODRewardsTable}}
    GROUP BY reward_hash, snapshot, token, avs, operator_set_id, operator
),

-- Step 2: Calculate total tokens actually distributed from the operator rewards table
total_distributed_tokens AS (
    SELECT
        reward_hash,
        snapshot,
        COALESCE(SUM(operator_tokens), 0) as distributed_tokens
    FROM {{.operatorRewardsTable}}
    GROUP BY reward_hash, snapshot
),

-- Step 3: Identify snapshots where distributed tokens = 0, refund all available tokens to AVS
snapshots_requiring_refund AS (
    SELECT
        tat.reward_hash,
        tat.snapshot,
        tat.token,
        tat.avs,
        tat.operator_set_id,
        tat.operator,
        tat.total_tokens as avs_tokens
    FROM total_available_tokens tat
    LEFT JOIN total_distributed_tokens tdt
        ON tat.reward_hash = tdt.reward_hash
        AND tat.snapshot = tdt.snapshot
    WHERE COALESCE(tdt.distributed_tokens, 0) = 0
)

SELECT * FROM snapshots_requiring_refund
`

func (rc *RewardsCalculator) GenerateGold17AvsOperatorSetUniqueStakeRewardsTable(snapshotDate string, forks config.ForkMap) error {
	rewardsV2_2Enabled, err := rc.globalConfig.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.2 is enabled", "error", err)
		return err
	}
	if !rewardsV2_2Enabled {
		rc.logger.Sugar().Infow("Rewards v2.2 is not enabled, skipping v2.2 table 17")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_17_AvsOperatorSetUniqueStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 AVS operator set unique stake rewards (refunds)",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
		zap.String("coloradoHardforkDate", forks[config.RewardsFork_Colorado].Date),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_17_goldAvsOperatorSetUniqueStakeRewardsQuery, map[string]interface{}{
		"destTableName":        destTableName,
		"activeODRewardsTable": allTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards],
		"operatorRewardsTable": allTableNames[rewardsUtils.Table_15_OperatorOperatorSetUniqueStakeRewards],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_avs_operator_set_unique_stake_rewards v2.2", "error", res.Error)
		return res.Error
	}
	return nil
}
