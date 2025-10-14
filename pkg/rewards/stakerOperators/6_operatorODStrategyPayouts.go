package stakerOperators

import (
	"fmt"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

// _6_operatorODStrategyPayoutQuery is the query that generates the operator OD strategy payouts.
//
// In OperatorDirectedRewards (OD), the operator is paid a reward set by the AVS irrespective of the strategies
// that are delegated to the operator. Because we cant break out by strategy, we can only take the amount that
// the operator is paid of ALL strategies for the given snapshot since it doesnt matter if there are 1 or N strategies
// delegated to the operator; the amount remains the same.
const _6_operatorODStrategyPayoutQuery = `
create table {{.destTableName}} as
select
	od.operator,
	od.reward_hash,
	od.snapshot,
	od.token,
	od.avs,
	od.strategy,
	od.multiplier,
	od.reward_submission_date,
	od.split_pct,
	od.operator_tokens
from {{.operatorODRewardAmountsTable}} as od
`

type OperatorODStrategyPayout struct {
	RewardHash           string
	Snapshot             time.Time
	Token                string
	Avs                  string
	Strategy             string
	Multiplier           string
	RewardSubmissionDate time.Time
	SplitPct             string
	OperatorTokens       string
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert6OperatorODStrategyPayouts(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	rewardsV2Enabled, err := sog.globalConfig.IsRewardsV2EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}
	if !rewardsV2Enabled {
		sog.logger.Sugar().Infow("Skipping 6_operatorODStrategyPayouts generation as rewards v2 is not enabled")
		return nil
	}
	destTableName := sog.getTempOperatorODStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	sog.logger.Sugar().Infow("Generating temp 6_operatorODStrategyPayouts",
		"cutoffDate", cutoffDate,
		"destTableName", destTableName,
	)

	// Drop existing temp table
	if err := sog.DropTempOperatorODStrategyPayoutTable(cutoffDate, generatedRewardsSnapshotId); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop existing temp operator OD strategy payout table", "error", err)
		return err
	}

	tempOperatorODRewardAmountsTable := sog.getTempOperatorODRewardAmountsTableName(cutoffDate, generatedRewardsSnapshotId)

	query, err := rewardsUtils.RenderQueryTemplate(_6_operatorODStrategyPayoutQuery, map[string]interface{}{
		"destTableName":                destTableName,
		"operatorODRewardAmountsTable": tempOperatorODRewardAmountsTable,
		"generatedRewardsSnapshotId":   generatedRewardsSnapshotId,
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 6_operatorODStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate temp 6_operatorODStrategyPayouts", "error", res.Error)
		return res.Error
	}
	return nil
}

func (sog *StakerOperatorsGenerator) getTempOperatorODStrategyPayoutTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_staker_operators_6_operator_od_strategy_payout_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (sog *StakerOperatorsGenerator) DropTempOperatorODStrategyPayoutTable(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := sog.getTempOperatorODStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := sog.db.Exec(query)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to drop temp operator OD strategy payout table", "error", res.Error)
		return res.Error
	}
	sog.logger.Sugar().Infow("Successfully dropped temp operator OD strategy payout table",
		zap.String("tempTableName", tempTableName),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	return nil
}

func (sog *StakerOperatorsGenerator) getTempOperatorODRewardAmountsTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_rewards_gold_8_operator_od_reward_amounts_%s_%d", camelDate, generatedRewardSnapshotId)
}
