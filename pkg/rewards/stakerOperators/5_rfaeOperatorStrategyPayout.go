package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"time"
)

/**
 * This view calculates the tokens returned to each operator for rewards_for_all_earners on a per-strategy basis
 *
 * 1. avs_opted_operators: Get the operators who have registered for an AVS for a given snapshot
 * 2. reward_snapshot_operators: Get the operators for the avs's strategy reward
 * 3. staker_delegated_operators: Get the stakers that were delegated to the operator for the snapshot
 * 4. staker_avs_strategy_shares: Get the shares for staker delegated to the operator
 * 5. rejoined_staker_strategies: Join the strategies that were not included in rewards_for_all_earners originally
 * 6. staker_strategy_weights: Calculate the weight of a staker for each of their strategies
 * 7. staker_strategy_weights_sum: Calculate sum of all staker_strategy_weight for each (staker, reward, snapshot)
 * 8. staker_strategy_proportions: Calculate staker strategy proportion of tokens for each reward and snapshot
 * 9. staker_strategy_tokens: Calculate the tokens returned to each staker on a per-strategy basis
 */

const _5_rfaeOperatorStrategyPayoutsQuery = `
WITH avs_opted_operators AS (
  SELECT DISTINCT
    snapshot,
    operator
  FROM operator_avs_registration_snapshots
),
-- Get the operators who will earn rewards for the reward submission at the given snapshot
reward_snapshot_operators as (
  SELECT
    ap.reward_hash,
    ap.snapshot,
    ap.token,
    ap.tokens_per_day,
    ap.avs,
    ap.strategy,
    ap.multiplier,
    ap.reward_type,
    aoo.operator
  FROM {{.activeRewardsTable}} ap
  JOIN avs_opted_operators aoo
  ON ap.snapshot = aoo.snapshot
  WHERE ap.reward_type = 'all_earners'
),
operator_strategy_shares AS (
  SELECT
    rso.*,
    oss.shares
  FROM reward_snapshot_operators rso
  JOIN operator_share_snapshots oss
  ON
    rso.operator = oss.operator AND
    rso.strategy = oss.strategy AND
    rso.snapshot = oss.snapshot
),
rejoined_operator_strategies AS (
  SELECT
    oss.*,
    rfao.operator_tokens
  FROM operator_strategy_shares oss
  JOIN {{.rfaeOperatorTable}} rfao
  ON
    oss.snapshot = rfao.snapshot AND
    oss.reward_hash = rfao.reward_hash AND
    oss.operator = rfao.operator
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  WHERE oss.shares > 0 and oss.multiplier > 0
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

type RfaeOperatorStrategyPayout struct {
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

func (osr *RfaeOperatorStrategyPayout) TableName() string {
	return "sot_5_rfae_operator_strategy_payout"
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert5RfaeOperatorStrategyPayout(cutoffDate string) error {
	sog.logger.Sugar().Infow("Generating and inserting 5_rfaeOperatorStrategyPayout",
		"cutoffDate", cutoffDate,
	)
	tableName := "sot_5_rfae_operator_strategy_payout"
	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)

	query, err := rewardsUtils.RenderQueryTemplate(_5_rfaeOperatorStrategyPayoutsQuery, map[string]interface{}{
		"activeRewardsTable": allTableNames[rewardsUtils.Table_1_ActiveRewards],
		"rfaeOperatorTable":  allTableNames[rewardsUtils.Table_6_RfaeOperators],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 5_rfaeOperatorStrategyPayoutsQuery query", "error", err)
		return err
	}

	err = rewardsUtils.GenerateAndInsertFromQuery(sog.db, tableName, query, nil, sog.logger)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to generate 5_rfaeOperatorStrategyPayoutsQuery", "error", err)
		return err
	}
	return nil
}

func (sog *StakerOperatorsGenerator) List5RfaeOperatorStrategyPayout() ([]*RfaeOperatorStrategyPayout, error) {
	var rewards []*RfaeOperatorStrategyPayout
	res := sog.db.Model(&RfaeOperatorStrategyPayout{}).Find(&rewards)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to list 5_rfaeOperatorStrategyPayoutsQuery", "error", res.Error)
		return nil, res.Error
	}
	return rewards, nil
}
