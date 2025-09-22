package stakerOperators

import (
	"time"

	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _12_stakerOperatorsStaging = `
insert into {{.destTableName}} as
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
  null::bigint as operator_set_id
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
  null::bigint as operator_set_id
FROM {{.sot2OperatorStrategyRewards}}

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
  null::bigint as operator_set_id
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
  null::bigint as operator_set_id
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
  null::bigint as operator_set_id
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
	null::bigint as operator_set_id
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
	null::bigint as operator_set_id
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
	null::bigint as operator_set_id
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

func (sog *StakerOperatorsGenerator) GenerateAndInsert12StakerOperator(cutoffDate string, generatedRewardsSnapshotId uint64) error {
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

	destTableName := rewardsUtils.Sot_13_StakerOperatorTable

	query, err := rewardsUtils.RenderQueryTemplate(_12_stakerOperatorsStaging, map[string]interface{}{
		"destTableName":                            destTableName,
		"rewardsV2Enabled":                         rewardsV2Enabled,
		"sot1StakerStrategyPayouts":                sog.getTempStakerStrategyPayoutsTableName(cutoffDate, generatedRewardsSnapshotId),
		"sot2OperatorStrategyRewards":              sog.getTempOperatorStrategyRewardsTableName(cutoffDate, generatedRewardsSnapshotId),
		"sot3RewardsForAllStrategyPayouts":         sog.getTempRewardsForAllStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId),
		"sot4RfaeStakerStrategyPayout":             sog.getTempRfaeStakerStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId),
		"sot5RfaeOperatorStrategyPayout":           sog.getTempRfaeOperatorStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId),
		"sot6OperatorODStrategyPayouts":            sog.getTempOperatorODStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId),
		"sot7StakerODStrategyPayouts":              sog.getTempStakerODStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId),
		"sot8AvsODStrategyPayouts":                 sog.getTempAvsODStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId),
		"rewardsV2_1Enabled":                       rewardsV2_1Enabled,
		"sot9OperatorODOperatorSetStrategyPayouts": sog.getTempOperatorODOperatorSetStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId),
		"sot10StakerODOperatorSetStrategyPayouts":  sog.getTempStakerODOperatorSetStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId),
		"sot11AvsODOperatorSetStrategyPayouts":     sog.getTempAvsODOperatorSetStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId),
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 12_stakerOperators query", "error", err)
		return err
	}

	res := sog.db.Exec(query)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 12_stakerOperators",
			zap.String("cutoffDate", cutoffDate),
			zap.Error(res.Error),
		)
		return res.Error
	}

	return nil
}
