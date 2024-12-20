package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"time"
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

func (sog *StakerOperatorsGenerator) GenerateAndInsert6OperatorODStrategyPayouts(cutoffDate string) error {
	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_6_OperatorODStrategyPayouts]

	sog.logger.Sugar().Infow("Generating and inserting 6_operatorODStrategyPayouts",
		"cutoffDate", cutoffDate,
	)

	if err := rewardsUtils.DropTableIfExists(sog.db, destTableName, sog.logger); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop table", "error", err)
		return err
	}

	rewardsTables, err := sog.FindRewardsTableNamesForSearchPattersn(map[string]string{
		rewardsUtils.Table_8_OperatorODRewardAmounts: rewardsUtils.GoldTableNameSearchPattern[rewardsUtils.Table_8_OperatorODRewardAmounts],
	}, cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to find staker operator table names", "error", err)
		return err
	}

	query, err := rewardsUtils.RenderQueryTemplate(_6_operatorODStrategyPayoutQuery, map[string]interface{}{
		"destTableName":                destTableName,
		"operatorODRewardAmountsTable": rewardsTables[rewardsUtils.Table_8_OperatorODRewardAmounts],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 6_operatorODStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 6_operatorODStrategyPayouts", "error", res.Error)
		return err
	}
	return nil
}
