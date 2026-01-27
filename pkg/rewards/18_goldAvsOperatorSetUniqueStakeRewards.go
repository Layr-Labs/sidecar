package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _18_goldAvsOperatorSetUniqueStakeRewardsQuery = `
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

-- Step 2: Calculate total operator tokens distributed per operator from the operator rewards table
operator_distributed_tokens AS (
    SELECT
        reward_hash,
        snapshot,
        avs,
        operator_set_id,
        operator,
        COALESCE(SUM(operator_tokens), 0) as operator_distributed
    FROM {{.operatorRewardsTable}}
    GROUP BY reward_hash, snapshot, avs, operator_set_id, operator
),

-- Step 3: Calculate total staker tokens distributed per operator from the staker rewards table
staker_distributed_tokens AS (
    SELECT
        reward_hash,
        snapshot,
        avs,
        operator_set_id,
        operator,
        COALESCE(SUM(staker_tokens), 0) as staker_distributed
    FROM {{.stakerRewardsTable}}
    GROUP BY reward_hash, snapshot, avs, operator_set_id, operator
),

-- Step 4: Identify operator-sets where total distributed tokens (operator + staker) = 0, refund those tokens to AVS
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
    LEFT JOIN operator_distributed_tokens odt
        ON tat.reward_hash = odt.reward_hash
        AND tat.snapshot = odt.snapshot
        AND tat.avs = odt.avs
        AND tat.operator_set_id = odt.operator_set_id
        AND tat.operator = odt.operator
    LEFT JOIN staker_distributed_tokens sdt
        ON tat.reward_hash = sdt.reward_hash
        AND tat.snapshot = sdt.snapshot
        AND tat.avs = sdt.avs
        AND tat.operator_set_id = sdt.operator_set_id
        AND tat.operator = sdt.operator
    WHERE COALESCE(odt.operator_distributed, 0) + COALESCE(sdt.staker_distributed, 0) = 0
)

SELECT * FROM snapshots_requiring_refund
`

func (rc *RewardsCalculator) GenerateGold18AvsOperatorSetUniqueStakeRewardsTable(snapshotDate string, forks config.ForkMap) error {
	rewardsV2_2Enabled, err := rc.globalConfig.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.2 is enabled", "error", err)
		return err
	}
	if !rewardsV2_2Enabled {
		rc.logger.Sugar().Infow("Rewards v2.2 is not enabled, skipping v2.2 table 18")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_18_AvsOperatorSetUniqueStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 AVS operator set unique stake rewards (refunds)",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
		zap.String("coloradoHardforkDate", forks[config.RewardsFork_Colorado].Date),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_18_goldAvsOperatorSetUniqueStakeRewardsQuery, map[string]interface{}{
		"destTableName":        destTableName,
		"activeODRewardsTable": allTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards],
		"operatorRewardsTable": allTableNames[rewardsUtils.Table_16_OperatorOperatorSetUniqueStakeRewards],
		"stakerRewardsTable":   allTableNames[rewardsUtils.Table_17_StakerOperatorSetUniqueStakeRewards],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query, sql.Named("coloradoHardforkDate", forks[config.RewardsFork_Colorado].Date))
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_avs_operator_set_unique_stake_rewards v2.2", "error", res.Error)
		return res.Error
	}
	return nil
}
