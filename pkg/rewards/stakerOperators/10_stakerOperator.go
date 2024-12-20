package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
	"time"
)

const _7_stakerOperator = `
insert into {{.destTableName}} (
	earner,
	operator,
	reward_type,
	avs,
	token,
	strategy,
	multiplier,
	shares,
	amount,
	reward_hash,
	snapshot
)
select
	earner,
	operator,
	reward_type,
	avs,
	token,
	strategy,
	multiplier,
	shares,
	amount,
	reward_hash,
	snapshot
from {{.stakerOperatorStaging}}
on conflict on constraint uniq_staker_operator do nothing;
`

type StakerOperator struct {
	Earner     string
	Operator   string
	RewardType string
	Avs        string
	Token      string
	Strategy   string
	Multiplier string
	Shares     string
	Amount     string
	RewardHash string
	Snapshot   time.Time
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert10StakerOperator(cutoffDate string) error {
	sog.logger.Sugar().Infow("Generating and inserting 10_stakerOperator",
		zap.String("cutoffDate", cutoffDate),
	)
	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := rewardsUtils.Sot_10_StakerOperatorTable

	sog.logger.Sugar().Infow("Generating 10_stakerOperator",
		zap.String("destTableName", destTableName),
		zap.String("cutoffDate", cutoffDate),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_7_stakerOperator, map[string]interface{}{
		"destTableName":         destTableName,
		"stakerOperatorStaging": allTableNames[rewardsUtils.Sot_9_StakerOperatorStaging],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 10_stakerOperator query", "error", err)
		return err
	}

	res := sog.db.Exec(query)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 10_stakerOperator",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(res.Error),
		)
	}

	return nil
}

func (sog *StakerOperatorsGenerator) List7StakerOperator() ([]*StakerOperator, error) {
	var rewards []*StakerOperator
	res := sog.db.Model(&StakerOperator{}).Find(&rewards)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to list 7_stakerOperator", "error", res.Error)
		return nil, res.Error
	}
	return rewards, nil
}
