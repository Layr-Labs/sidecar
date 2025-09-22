package stakerOperators

import (
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _10_stakerODOperatorSetStrategyPayoutQuery = `
create table {{.destTableName}} as
select
    staker,
    operator,
    avs,
	operator_set_id,
    token,
    strategy,
    multiplier,
    shares,
    staker_tokens,
    reward_hash,
    snapshot
from {{.stakerODOperatorSetRewardAmountsTable}}
`

func (sog *StakerOperatorsGenerator) GenerateAndInsert10StakerODOperatorSetStrategyPayouts(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	rewardsV2_1Enabled, err := sog.globalConfig.IsRewardsV2_1EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}
	if !rewardsV2_1Enabled {
		sog.logger.Sugar().Infow("Skipping 10_stakerODOperatorSetStrategyPayouts generation as rewards v2.1 is not enabled")
		return nil
	}

	destTableName := sog.getTempStakerODOperatorSetStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	sog.logger.Sugar().Infow("Generating temp 10_stakerODOperatorSetStrategyPayouts",
		"cutoffDate", cutoffDate,
		"destTableName", destTableName,
	)

	// Drop existing temp table
	if err := sog.DropTempStakerODOperatorSetStrategyPayoutTable(cutoffDate, generatedRewardsSnapshotId); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop existing temp staker OD operator set strategy payout table", "error", err)
		return err
	}

	tempStakerODOperatorSetRewardAmountsTable := sog.getTempStakerODOperatorSetRewardAmountsTableName(cutoffDate, generatedRewardsSnapshotId)

	query, err := rewardsUtils.RenderQueryTemplate(_10_stakerODOperatorSetStrategyPayoutQuery, map[string]interface{}{
		"destTableName":                         destTableName,
		"stakerODOperatorSetRewardAmountsTable": tempStakerODOperatorSetRewardAmountsTable,
		"generatedRewardsSnapshotId":            generatedRewardsSnapshotId,
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 10_stakerODOperatorSetStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate temp 10_stakerODOperatorSetStrategyPayouts", "error", res.Error)
		return res.Error
	}
	return nil
}

func (sog *StakerOperatorsGenerator) getTempStakerODOperatorSetStrategyPayoutTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_staker_operators_10_staker_od_operator_set_strategy_payout_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (sog *StakerOperatorsGenerator) DropTempStakerODOperatorSetStrategyPayoutTable(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := sog.getTempStakerODOperatorSetStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := sog.db.Exec(query)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to drop temp staker OD operator set strategy payout table", "error", res.Error)
		return res.Error
	}
	sog.logger.Sugar().Infow("Successfully dropped temp staker OD operator set strategy payout table",
		zap.String("tempTableName", tempTableName),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	return nil
}

func (sog *StakerOperatorsGenerator) getTempStakerODOperatorSetRewardAmountsTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_rewards_gold_13_staker_od_operator_set_reward_amounts_%s_%d", camelDate, generatedRewardSnapshotId)
}
