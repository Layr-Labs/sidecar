package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
	"time"
)

const _6_stakerOperatorsStaging = `
create table {{.destTableName}} as
SELECT 
  staker as earner,
  operator,
  'staker_reward' as reward_type,
  avs,
  token,
  strategy,
  multiplier,
  shares,
  staker_strategy_tokens as amount,
  reward_hash,
  snapshot
FROM sot_1_staker_strategy_payouts

UNION ALL

SELECT
  operator as earner,
  operator as operator,
  'operator_reward' as reward_type,
  avs,
  token,
  strategy,
  multiplier,
  shares,
  operator_strategy_tokens as amount,
  reward_hash,
  snapshot
FROM sot_2_operator_strategy_rewards

UNION all

SELECT
  staker as earner,
  '0x0000000000000000000000000000000000000000' as operator,
  'reward_for_all' as reward_type,
  avs,
  token,
  strategy,
  multiplier,
  shares,
  staker_strategy_tokens as amount,
  reward_hash,
  snapshot
FROM sot_3_rewards_for_all_strategy_payout

UNION ALL

SELECT
  staker as earner,
  operator,
  'rfae_staker' as reward_type,
  avs,
  token,
  strategy,
  multiplier,
  shares,
  staker_strategy_tokens as amount,
  reward_hash,
  snapshot
FROM sot_4_rfae_staker_strategy_payout

UNION ALL

SELECT
  operator as earner,
  operator as operator,
  'rfae_operator' as reward_type,
  avs,
  token,
  strategy,
  multiplier,
  shares,
  operator_strategy_tokens as amount,
  reward_hash,
  snapshot
FROM sot_5_rfae_operator_strategy_payout
`

type StakerOperatorStaging struct {
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

func (sog *StakerOperatorsGenerator) GenerateAndInsert6StakerOperatorStaging(cutoffDate string) error {
	sog.logger.Sugar().Infow("Generating and inserting 6_stakerOperatorsStaging",
		zap.String("cutoffDate", cutoffDate),
	)
	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_6_StakerOperatorStaging]

	sog.logger.Sugar().Infow("Generating 6_stakerOperatorsStaging",
		zap.String("destTableName", destTableName),
		zap.String("cutoffDate", cutoffDate),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_6_stakerOperatorsStaging, map[string]interface{}{
		"destTableName": destTableName,
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 6_stakerOperatorsStaging query", "error", err)
		return err
	}

	res := sog.db.Exec(query)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 6_stakerOperatorsStaging",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(res.Error),
		)
		return res.Error
	}

	return nil
}

func (sog *StakerOperatorsGenerator) List6StakerOperatorStaging() ([]*StakerOperatorStaging, error) {
	var rewards []*StakerOperatorStaging
	res := sog.db.Model(&StakerOperatorStaging{}).Find(&rewards)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to list 6_stakerOperatorsStaging", "error", res.Error)
		return nil, res.Error
	}
	return rewards, nil
}
