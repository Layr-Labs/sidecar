package stakerOperators

import (
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

// _8_avsODStrategyPayoutQuery is the query that generates the 8_avsODStrategyPayouts table
//
// AVS operator directed rewards are not actually rewards, but refunds for the case when the operator
// defined in the rewards-v2 submission wasnt delegated at the time of the snapshot. Since operator rewards
// in rewards-v2 are not based on strategy and are a lump sum between the AVS and operator, the refund
// is also the same; a lump sum BACK to the AVS that was originally intended for the operator. As such,
// there is no strategy, shares or multiplier fields to represent.
const _8_avsODStrategyPayoutQuery = `
create table {{.destTableName}} as
select
	reward_hash,
	snapshot,
	token,
	avs,
	operator,
	avs_tokens
from {{.avsODRewardAmountsTable}}
`

type AvsODStrategyPayout struct {
	RewardHash string
	Snapshot   string
	Token      string
	Avs        string
	Operator   string
	AvsTokens  string
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert8AvsODStrategyPayouts(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	rewardsV2Enabled, err := sog.globalConfig.IsRewardsV2EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}
	if !rewardsV2Enabled {
		sog.logger.Sugar().Infow("Skipping 8_avsODStrategyPayouts generation as rewards v2 is not enabled")
		return nil
	}

	destTableName := sog.getTempAvsODStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	sog.logger.Sugar().Infow("Generating temp 8_avsODStrategyPayouts",
		"cutoffDate", cutoffDate,
		"destTableName", destTableName,
	)

	// Drop existing temp table
	if err := sog.DropTempAvsODStrategyPayoutTable(cutoffDate, generatedRewardsSnapshotId); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop existing temp AVS OD strategy payout table", "error", err)
		return err
	}

	tempAvsODRewardAmountsTable := sog.getTempAvsODRewardAmountsTableName(cutoffDate, generatedRewardsSnapshotId)

	query, err := rewardsUtils.RenderQueryTemplate(_8_avsODStrategyPayoutQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"avsODRewardAmountsTable":    tempAvsODRewardAmountsTable,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 8_avsODStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate temp 8_avsODStrategyPayouts", "error", res.Error)
		return res.Error
	}
	return nil
}

func (sog *StakerOperatorsGenerator) getTempAvsODStrategyPayoutTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_staker_operators_8_avs_od_strategy_payout_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (sog *StakerOperatorsGenerator) DropTempAvsODStrategyPayoutTable(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := sog.getTempAvsODStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := sog.db.Exec(query)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to drop temp AVS OD strategy payout table", "error", res.Error)
		return res.Error
	}
	sog.logger.Sugar().Infow("Successfully dropped temp AVS OD strategy payout table",
		zap.String("tempTableName", tempTableName),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	return nil
}

func (sog *StakerOperatorsGenerator) getTempAvsODRewardAmountsTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_rewards_gold_10_avs_od_reward_amounts_%s_%d", camelDate, generatedRewardSnapshotId)
}
