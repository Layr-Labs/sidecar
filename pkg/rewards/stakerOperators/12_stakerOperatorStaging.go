package stakerOperators

import (
	"time"

	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _12_stakerOperatorsStaging = `
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
  snapshot,
  null as operator_set_id
FROM {{.sot1StakerStrategyPayouts}}

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
  snapshot,
  null as operator_set_id
FROM {{.sot2OperatorStrategyPayouts}}

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
  snapshot,
  null as operator_set_id
FROM {{.sot3RewardsForAllStrategyPayouts}}

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
  snapshot,
  null as operator_set_id
FROM {{.sot4RfaeStakerStrategyPayout}}

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
  snapshot,
  null as operator_set_id
FROM {{.sot5RfaeOperatorStrategyPayout}}

{{ if .rewardsV2Enabled }}

UNION ALL

SELECT
	operator as earner,
	operator as operator,
	'operator_od_reward' as reward_type,
	avs,
	token,
	'0x0000000000000000000000000000000000000000' as strategy,
	'0' as multiplier,
	'0' as shares,
	operator_tokens as amount,
	reward_hash,
	snapshot,
	null as operator_set_id
from {{.sot6OperatorODStrategyPayouts}}

UNION ALL

SELECT
	staker as earner,
	operator,
	'staker_od_reward' as reward_type,
	avs,
	token,
	strategy,
	multiplier,
	shares,
	staker_tokens as amount,
	reward_hash,
	snapshot,
	null as operator_set_id
from {{.sot7StakerODStrategyPayouts}}

UNION ALL

SELECT
	avs as earner,
	operator,
	'avs_od_reward' as reward_type,
	avs,
	token,
	'0x0000000000000000000000000000000000000000' as strategy,
	'0' as multiplier,
	'0' as shares,
	avs_tokens as amount,
	reward_hash,
	snapshot,
	null as operator_set_id
from {{.sot8AvsODStrategyPayouts}}

{{ end }}

{{ if .rewardsV2_1Enabled }}

UNION ALL

SELECT
	operator as earner,
	operator as operator,
	'operator_od_operator_set_reward' as reward_type,
	avs,
	token,
	'0x0000000000000000000000000000000000000000' as strategy,
	'0' as multiplier,
	'0' as shares,
	operator_tokens as amount,
	reward_hash,
	snapshot,
	operator_set_id
from {{.sot9OperatorODOperatorSetStrategyPayouts}}

UNION ALL

SELECT
	staker as earner,
	operator,
	'staker_od_operator_set_reward' as reward_type,
	avs,
	token,
	strategy,
	multiplier,
	shares,
	staker_tokens as amount,
	reward_hash,
	snapshot,
	operator_set_id
from {{.sot10StakerODOperatorSetStrategyPayouts}}

UNION ALL

SELECT
	avs as earner,
	operator,
	'avs_od_operator_set_reward' as reward_type,
	avs,
	token,
	'0x0000000000000000000000000000000000000000' as strategy,
	'0' as multiplier,
	'0' as shares,
	avs_tokens as amount,
	reward_hash,
	snapshot,
	operator_set_id
from {{.sot11AvsODOperatorSetStrategyPayouts}}

{{ end }}
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

func (sog *StakerOperatorsGenerator) GenerateAndInsert12StakerOperatorStaging(cutoffDate string) error {
	rewardsV2Enabled, err := sog.globalConfig.IsRewardsV2EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}

	rewardsV2_1Enabled, err := sog.globalConfig.IsRewardsV2_1EnabledForCutoffDate(cutoffDate)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}

	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_12_StakerOperatorStaging]

	if err := rewardsUtils.DropTableIfExists(sog.db, destTableName, sog.logger); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop table", "error", err)
		return err
	}

	sog.logger.Sugar().Infow("Generating and inserting 12_stakerOperatorsStaging",
		zap.String("cutoffDate", cutoffDate),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_12_stakerOperatorsStaging, map[string]interface{}{
		"destTableName":                            destTableName,
		"rewardsV2Enabled":                         rewardsV2Enabled,
		"sot1StakerStrategyPayouts":                allTableNames[rewardsUtils.Sot_1_StakerStrategyPayouts],
		"sot2OperatorStrategyPayouts":              allTableNames[rewardsUtils.Sot_2_OperatorStrategyPayouts],
		"sot3RewardsForAllStrategyPayouts":         allTableNames[rewardsUtils.Sot_3_RewardsForAllStrategyPayout],
		"sot4RfaeStakerStrategyPayout":             allTableNames[rewardsUtils.Sot_4_RfaeStakers],
		"sot5RfaeOperatorStrategyPayout":           allTableNames[rewardsUtils.Sot_5_RfaeOperators],
		"sot6OperatorODStrategyPayouts":            allTableNames[rewardsUtils.Sot_6_OperatorODStrategyPayouts],
		"sot7StakerODStrategyPayouts":              allTableNames[rewardsUtils.Sot_7_StakerODStrategyPayouts],
		"sot8AvsODStrategyPayouts":                 allTableNames[rewardsUtils.Sot_8_AvsODStrategyPayouts],
		"rewardsV2_1Enabled":                       rewardsV2_1Enabled,
		"sot9OperatorODOperatorSetStrategyPayouts": allTableNames[rewardsUtils.Sot_9_OperatorODOperatorSetStrategyPayouts],
		"sot10StakerODOperatorSetStrategyPayouts":  allTableNames[rewardsUtils.Sot_10_StakerODOperatorSetStrategyPayouts],
		"sot11AvsODOperatorSetStrategyPayouts":     allTableNames[rewardsUtils.Sot_11_AvsODOperatorSetStrategyPayouts],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 12_stakerOperatorsStaging query", "error", err)
		return err
	}

	res := sog.db.Exec(query)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 12_stakerOperatorsStaging",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(res.Error),
		)
		return res.Error
	}

	return nil
}
