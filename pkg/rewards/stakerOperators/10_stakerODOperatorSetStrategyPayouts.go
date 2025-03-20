package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
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

func (sog *StakerOperatorsGenerator) GenerateAndInsert10StakerODOperatorSetStrategyPayouts(cutoffDate string) error {
	rewardsV2_1Enabled, err := sog.globalConfig.IsRewardsV2_1EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}
	if !rewardsV2_1Enabled {
		sog.logger.Sugar().Infow("Skipping 10_stakerODOperatorSetStrategyPayouts generation as rewards v2.1 is not enabled")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_10_StakerODOperatorSetStrategyPayouts]

	sog.logger.Sugar().Infow("Generating and inserting 10_stakerODOperatorSetStrategyPayouts",
		"cutoffDate", cutoffDate,
	)

	if err := rewardsUtils.DropTableIfExists(sog.db, destTableName, sog.logger); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop table", "error", err)
		return err
	}

	rewardsTables, err := sog.FindRewardsTableNamesForSearchPattersn(map[string]string{
		rewardsUtils.Table_13_StakerODOperatorSetRewardAmounts: rewardsUtils.GoldTableNameSearchPattern[rewardsUtils.Table_13_StakerODOperatorSetRewardAmounts],
	}, cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to find staker operator table names", "error", err)
		return err
	}

	query, err := rewardsUtils.RenderQueryTemplate(_10_stakerODOperatorSetStrategyPayoutQuery, map[string]interface{}{
		"destTableName":                         destTableName,
		"stakerODOperatorSetRewardAmountsTable": rewardsTables[rewardsUtils.Table_13_StakerODOperatorSetRewardAmounts],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 10_stakerODOperatorSetStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 10_stakerODOperatorSetStrategyPayouts", "error", res.Error)
		return err
	}
	return nil
}
