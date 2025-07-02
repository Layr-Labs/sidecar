package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
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

func (sog *StakerOperatorsGenerator) GenerateAndInsert9OperatorODOperatorSetStrategyPayouts(cutoffDate string) error {
	rewardsV2_1Enabled, err := sog.globalConfig.IsRewardsV2_1EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}
	if !rewardsV2_1Enabled {
		sog.logger.Sugar().Infow("Skipping 9_operatorODOperatorSetStrategyPayouts generation as rewards v2.1 is not enabled")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_9_OperatorODOperatorSetStrategyPayouts]

	sog.logger.Sugar().Infow("Generating and inserting 9_operatorODOperatorSetStrategyPayouts",
		"cutoffDate", cutoffDate,
	)

	if err := rewardsUtils.DropTableIfExists(sog.db, destTableName, sog.logger); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop table", "error", err)
		return err
	}

	query, err := rewardsUtils.RenderQueryTemplate(_9_operatorODOperatorSetStrategyPayoutQuery, map[string]interface{}{
		"destTableName": destTableName,
		"operatorODOperatorSetRewardAmountsTable": rewardsUtils.RewardsTable_12_OperatorODOperatorSetRewardAmounts,
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 9_operatorODOperatorSetStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 9_operatorODOperatorSetStrategyPayouts", "error", res.Error)
		return err
	}
	return nil
}
