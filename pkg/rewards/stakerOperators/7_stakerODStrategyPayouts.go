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
	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_7_StakerODStrategyPayouts]

	sog.logger.Sugar().Infow("Generating and inserting 7_stakerODStrategyPayouts",
		"cutoffDate", cutoffDate,
	)

	query, err := rewardsUtils.RenderQueryTemplate(_7_stakerODStrategyPayoutQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"stakerODRewardAmountsTable": allTableNames[rewardsUtils.Table_9_StakerODRewardAmounts],
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
