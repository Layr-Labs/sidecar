package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
)

const _11_avsODOperatorSetStrategyPayoutQuery = `
create table {{.destTableName}} as
select
	reward_hash,
	snapshot,
	token,
	avs,
	operator_set_id,
	operator,
	avs_tokens
from {{.avsODOperatorSetRewardAmountsTable}}
where generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
`

func (sog *StakerOperatorsGenerator) GenerateAndInsert11AvsODOperatorSetStrategyPayouts(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	rewardsV2_1Enabled, err := sog.globalConfig.IsRewardsV2_1EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}
	if !rewardsV2_1Enabled {
		sog.logger.Sugar().Infow("Skipping 11_avsODOperatorSetStrategyPayouts generation as rewards v2.1 is not enabled")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_11_AvsODOperatorSetStrategyPayouts]

	sog.logger.Sugar().Infow("Generating and inserting 11_avsODOperatorSetStrategyPayouts",
		"cutoffDate", cutoffDate,
	)

	if err := rewardsUtils.DropTableIfExists(sog.db, destTableName, sog.logger); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop table", "error", err)
		return err
	}

	query, err := rewardsUtils.RenderQueryTemplate(_11_avsODOperatorSetStrategyPayoutQuery, map[string]interface{}{
		"destTableName":                      destTableName,
		"avsODOperatorSetRewardAmountsTable": rewardsUtils.RewardsTable_14_AvsODOperatorSetRewardAmounts,
		"generatedRewardsSnapshotId":         generatedRewardsSnapshotId,
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 11_avsODOperatorSetStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 11_avsODOperatorSetStrategyPayouts", "error", res.Error)
		return err
	}
	return nil
}
