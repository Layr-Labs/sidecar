package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"time"
)

const _2_operatorStrategyRewardsQuery = `
WITH reward_snapshot_operators as (
  SELECT 
    ap.reward_hash,
    ap.snapshot,
    ap.token,
    ap.tokens_per_day,
    ap.avs,
    ap.strategy,
    ap.multiplier,
    ap.reward_type,
    oar.operator
  FROM {{.activeRewardsTable}} ap
  JOIN operator_avs_registration_snapshots oar
  ON ap.avs = oar.avs and ap.snapshot = oar.snapshot
  WHERE ap.reward_type = 'avs'
),
operator_restaked_strategies AS (
  SELECT
    rso.*
  FROM reward_snapshot_operators rso
  JOIN operator_avs_strategy_snapshots oas
  ON
    rso.operator = oas.operator AND
    rso.avs = oas.avs AND
    rso.strategy = oas.strategy AND
    rso.snapshot = oas.snapshot
),
operator_avs_strategy_shares AS (
  SELECT
    oas.*,
    oss.shares
  FROM operator_restaked_strategies oas
  JOIN operator_share_snapshots oss
  ON
    oas.operator = oss.operator AND
    oas.strategy = oss.strategy AND
    oas.snapshot = oss.snapshot
),
rejoined_operator_strategies AS (
  SELECT
    oass.*,
    opa.operator_tokens
  FROM operator_avs_strategy_shares oass
  JOIN {{.operatorRewardAmountsTable}} opa
  ON
    oass.snapshot = opa.snapshot AND
    oass.reward_hash = opa.reward_hash AND
    oass.operator = opa.operator
  WHERE oass.shares > 0 AND oass.multiplier != 0
),
-- Calculate the weight of a operator for each of their strategies
operator_strategy_weights AS (
  SELECT *,
    multiplier * shares AS operator_strategy_weight
  FROM rejoined_operator_strategies
  ORDER BY reward_hash, snapshot, operator, strategy
),
-- Calculate sum of each operator operator_strategy_weight for each reward and snapshot for a given operator
operator_strategy_weights_sum AS (
  SELECT *,
    SUM(operator_strategy_weight) OVER (PARTITION BY operator, reward_hash, snapshot) as operator_total_strategy_weight
  FROM operator_strategy_weights
),
-- Calculate operator strategy proportion of tokens for each reward and snapshot
operator_strategy_proportions AS (
  SELECT *,
    FLOOR((operator_strategy_weight / operator_total_strategy_weight) * 1000000000000000) / 1000000000000000 as operator_strategy_proportion
  FROM operator_strategy_weights_sum
),
operator_strategy_tokens AS (
  SELECT *,
    floor(operator_strategy_proportion * operator_tokens) as operator_strategy_tokens
  FROM operator_strategy_proportions
)
SELECT * FROM operator_strategy_tokens
`

type OperatorStrategyRewards struct {
	RewardHash                  string
	Snapshot                    time.Time
	Token                       string
	TokensPerDay                float64
	Avs                         string
	Strategy                    string
	Multiplier                  string
	RewardType                  string
	Operator                    string
	Shares                      string
	OperatorTokens              string
	OperatorStrategyWeight      string
	OperatorTotalStrategyWeight string
	OperatorStrategyProportion  string
	OperatorStrategyTokens      string
}

func (osr *OperatorStrategyRewards) TableName() string {
	return "sot_2_operator_strategy_rewards"
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert2OperatorStrategyRewards(cutoffDate string) error {
	sog.logger.Sugar().Infow("Generating and inserting 2_operatorStrategyRewards",
		"cutoffDate", cutoffDate,
	)
	tableName := "sot_2_operator_strategy_rewards"
	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)

	query, err := rewardsUtils.RenderQueryTemplate(_2_operatorStrategyRewardsQuery, map[string]string{
		"activeRewardsTable":         allTableNames[rewardsUtils.Table_1_ActiveRewards],
		"operatorRewardAmountsTable": allTableNames[rewardsUtils.Table_3_OperatorRewardAmounts],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 2_operatorStrategyRewards query", "error", err)
		return err
	}

	err = rewardsUtils.GenerateAndInsertFromQuery(sog.db, tableName, query, nil, sog.logger)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to generate 2_operatorStrategyRewards", "error", err)
		return err
	}
	return nil
}

func (sog *StakerOperatorsGenerator) List2OperatorStrategyRewards() ([]*OperatorStrategyRewards, error) {
	var rewards []*OperatorStrategyRewards
	res := sog.db.Model(&OperatorStrategyRewards{}).Find(&rewards)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to list 2_operatorStrategyRewards", "error", res.Error)
		return nil, res.Error
	}
	return rewards, nil
}
