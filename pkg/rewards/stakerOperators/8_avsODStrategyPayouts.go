package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
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

func (sog *StakerOperatorsGenerator) GenerateAndInsert8AvsODStrategyPayouts(cutoffDate string) error {
	rewardsV2Enabled, err := sog.globalConfig.IsRewardsV2EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}
	if !rewardsV2Enabled {
		sog.logger.Sugar().Infow("Skipping 8_avsODStrategyPayouts generation as rewards v2 is not enabled")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_8_AvsODStrategyPayouts]

	sog.logger.Sugar().Infow("Generating and inserting 8_avsODStrategyPayouts",
		"cutoffDate", cutoffDate,
	)

	if err := rewardsUtils.DropTableIfExists(sog.db, destTableName, sog.logger); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop table", "error", err)
		return err
	}

	rewardsTables, err := sog.FindRewardsTableNamesForSearchPattersn(map[string]string{
		rewardsUtils.Table_10_AvsODRewardAmounts: rewardsUtils.GoldTableNameSearchPattern[rewardsUtils.Table_10_AvsODRewardAmounts],
	}, cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to find staker operator table names", "error", err)
		return err
	}

	query, err := rewardsUtils.RenderQueryTemplate(_8_avsODStrategyPayoutQuery, map[string]interface{}{
		"destTableName":           destTableName,
		"avsODRewardAmountsTable": rewardsTables[rewardsUtils.Table_10_AvsODRewardAmounts],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 8_avsODStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 8_avsODStrategyPayouts", "error", res.Error)
		return err
	}
	return nil
}
