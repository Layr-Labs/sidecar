package stakerOperators

import (
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _9_operatorODOperatorSetStrategyPayoutQuery = `
create table {{.destTableName}} as
select
	operator,
	reward_hash,
	snapshot,
	token,
	avs,
	operator_set_id,
	strategy,
	multiplier,
	reward_submission_date,
	split_pct,
	operator_tokens
from {{.operatorODOperatorSetRewardAmountsTable}}
`

func (sog *StakerOperatorsGenerator) GenerateAndInsert9OperatorODOperatorSetStrategyPayouts(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	rewardsV2_1Enabled, err := sog.globalConfig.IsRewardsV2_1EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}
	if !rewardsV2_1Enabled {
		sog.logger.Sugar().Infow("Skipping 9_operatorODOperatorSetStrategyPayouts generation as rewards v2.1 is not enabled")
		return nil
	}

	destTableName := sog.getTempOperatorODOperatorSetStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	sog.logger.Sugar().Infow("Generating temp 9_operatorODOperatorSetStrategyPayouts",
		"cutoffDate", cutoffDate,
		"destTableName", destTableName,
	)

	// Drop existing temp table
	if err := sog.DropTempOperatorODOperatorSetStrategyPayoutTable(cutoffDate, generatedRewardsSnapshotId); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop existing temp operator OD operator set strategy payout table", "error", err)
		return err
	}

	tempOperatorODOperatorSetRewardAmountsTable := sog.getTempOperatorODOperatorSetRewardAmountsTableName(cutoffDate, generatedRewardsSnapshotId)

	query, err := rewardsUtils.RenderQueryTemplate(_9_operatorODOperatorSetStrategyPayoutQuery, map[string]interface{}{
		"destTableName": destTableName,
		"operatorODOperatorSetRewardAmountsTable": tempOperatorODOperatorSetRewardAmountsTable,
		"generatedRewardsSnapshotId":              generatedRewardsSnapshotId,
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 9_operatorODOperatorSetStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate temp 9_operatorODOperatorSetStrategyPayouts", "error", res.Error)
		return res.Error
	}
	return nil
}

func (sog *StakerOperatorsGenerator) getTempOperatorODOperatorSetStrategyPayoutTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_staker_operators_9_operator_od_operator_set_strategy_payout_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (sog *StakerOperatorsGenerator) DropTempOperatorODOperatorSetStrategyPayoutTable(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := sog.getTempOperatorODOperatorSetStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := sog.db.Exec(query)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to drop temp operator OD operator set strategy payout table", "error", res.Error)
		return res.Error
	}
	sog.logger.Sugar().Infow("Successfully dropped temp operator OD operator set strategy payout table",
		zap.String("tempTableName", tempTableName),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	return nil
}

func (sog *StakerOperatorsGenerator) getTempOperatorODOperatorSetRewardAmountsTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_rewards_gold_12_operator_od_operator_set_reward_amounts_%s_%d", camelDate, generatedRewardSnapshotId)
}
