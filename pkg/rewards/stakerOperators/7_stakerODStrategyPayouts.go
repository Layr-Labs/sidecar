package stakerOperators

import (
	"fmt"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

// _7_stakerODStrategyPayoutQuery is a constant value that represents a query.
//
// Unlike rewards v1 where all staker amounts are pre-summed across operator/strategies,
// in rewards v2 they are already represented at the staker/operator/strategy/reward_hash level
// so adding them to the staker-operator table is a simple select from what we have already.
const _7_stakerODStrategyPayoutQuery = `
create table {{.destTableName}} as
select
    staker,
    operator,
    avs,
    token,
    strategy,
    multiplier,
    shares,
    staker_tokens,
    reward_hash,
    snapshot
from {{.stakerODRewardAmountsTable}}
`

type StakerODStrategyPayout struct {
	Staker       string
	Operator     string
	Avs          string
	Token        string
	Strategy     string
	Multiplier   string
	Shares       string
	StakerTokens string
	RewardHash   string
	Snapshot     time.Time
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert7StakerODStrategyPayouts(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	rewardsV2Enabled, err := sog.globalConfig.IsRewardsV2EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}
	if !rewardsV2Enabled {
		sog.logger.Sugar().Infow("Skipping 7_stakerODStrategyPayouts generation as rewards v2 is not enabled")
		return nil
	}

	destTableName := sog.getTempStakerODStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	sog.logger.Sugar().Infow("Generating temp 7_stakerODStrategyPayouts",
		"cutoffDate", cutoffDate,
		"destTableName", destTableName,
	)

	// Drop existing temp table
	if err := sog.DropTempStakerODStrategyPayoutTable(cutoffDate, generatedRewardsSnapshotId); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop existing temp staker OD strategy payout table", "error", err)
		return err
	}

	tempStakerODRewardAmountsTable := sog.getTempStakerODRewardAmountsTableName(cutoffDate, generatedRewardsSnapshotId)

	query, err := rewardsUtils.RenderQueryTemplate(_7_stakerODStrategyPayoutQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"stakerODRewardAmountsTable": tempStakerODRewardAmountsTable,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 7_stakerODStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate temp 7_stakerODStrategyPayouts", "error", res.Error)
		return res.Error
	}
	return nil
}

func (sog *StakerOperatorsGenerator) getTempStakerODStrategyPayoutTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_staker_operators_7_staker_od_strategy_payout_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (sog *StakerOperatorsGenerator) DropTempStakerODStrategyPayoutTable(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := sog.getTempStakerODStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := sog.db.Exec(query)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to drop temp staker OD strategy payout table", "error", res.Error)
		return res.Error
	}
	sog.logger.Sugar().Infow("Successfully dropped temp staker OD strategy payout table",
		zap.String("tempTableName", tempTableName),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	return nil
}

func (sog *StakerOperatorsGenerator) getTempStakerODRewardAmountsTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_rewards_gold_9_staker_od_reward_amounts_%s_%d", camelDate, generatedRewardSnapshotId)
}
