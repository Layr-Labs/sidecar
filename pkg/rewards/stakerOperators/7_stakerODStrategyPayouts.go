package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"time"
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

func (sog *StakerOperatorsGenerator) GenerateAndInsert7StakerODStrategyPayouts(cutoffDate string) error {
	rewardsV2Enabled, err := sog.globalConfig.IsRewardsV2EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}
	if !rewardsV2Enabled {
		sog.logger.Sugar().Infow("Skipping 7_stakerODStrategyPayouts generation as rewards v2 is not enabled")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_7_StakerODStrategyPayouts]

	sog.logger.Sugar().Infow("Generating and inserting 7_stakerODStrategyPayouts",
		"cutoffDate", cutoffDate,
	)

	if err := rewardsUtils.DropTableIfExists(sog.db, destTableName, sog.logger); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop table", "error", err)
		return err
	}

	rewardsTables, err := sog.FindRewardsTableNamesForSearchPattersn(map[string]string{
		rewardsUtils.Table_9_StakerODRewardAmounts: rewardsUtils.GoldTableNameSearchPattern[rewardsUtils.Table_9_StakerODRewardAmounts],
	}, cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to find staker operator table names", "error", err)
		return err
	}

	query, err := rewardsUtils.RenderQueryTemplate(_7_stakerODStrategyPayoutQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"stakerODRewardAmountsTable": rewardsTables[rewardsUtils.Table_9_StakerODRewardAmounts],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 7_stakerODStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 7_stakerODStrategyPayouts", "error", res.Error)
		return err
	}
	return nil
}
