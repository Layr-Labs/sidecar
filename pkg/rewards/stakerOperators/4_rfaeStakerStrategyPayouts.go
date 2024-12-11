package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"time"
)

/**
 * This table calculates the tokens for each staker from the pay for all rewards on a per-strategy basis
 *
 * Reward_snapshot_stakers: Get the stakers that are being paid out for a given snapshot
 * Rejoined_staker_strategies: Join the strategies that were not included in staker_rewards originally
 * Staker_strategy_weights: Calculate the weight of a staker for each of their strategies
 * Staker_strategy_weights_sum: Calculate sum of all staker_strategy_weight for each rewards and snapshot across all relevant strategies and stakers
 * Staker_strategy_proportions: Calculate staker strategy proportion of tokens for each rewards and snapshot
 * Staker_strategy_p4a_tokens: Calculate the tokens for each staker from the pay for all rewards on a per-strategy basis
 */
const _4_rfaeStakerStrategyPayoutsQuery = `
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
-- Get the stakers that were delegated to the operator for the snapshot
staker_delegated_operators AS (
  SELECT
    rso.*,
    sds.staker
  FROM reward_snapshot_operators rso
  JOIN staker_delegation_snapshots sds
  ON
    rso.operator = sds.operator AND
    rso.snapshot = sds.snapshot
),
-- Get the shares of each strategy the staker has delegated to the operator
staker_strategy_shares AS (
  SELECT
    sdo.*,
    sss.shares
  FROM staker_delegated_operators sdo
  JOIN staker_share_snapshots sss
  ON
    sdo.staker = sss.staker AND
    sdo.snapshot = sss.snapshot AND
    sdo.strategy = sss.strategy
),
-- Join the strategies that were not included in rfae_stakers originally
rejoined_staker_strategies AS (
  SELECT
    sss.*,
    rfas.staker_tokens
  FROM staker_strategy_shares sss
  JOIN {{.rfaeStakerTable}} rfas
  ON
    sss.snapshot = rfas.snapshot AND
    sss.reward_hash = rfas.reward_hash AND
    sss.staker = rfas.staker
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  WHERE sss.shares > 0 and sss.multiplier > 0
),
-- Calculate the weight of a staker for each of their strategies
staker_strategy_weights AS (
  SELECT *,
    multiplier * shares AS staker_strategy_weight
  FROM rejoined_staker_strategies
  ORDER BY reward_hash, snapshot, staker, strategy
),
-- Calculate sum of all staker_strategy_weight for each reward and snapshot across all relevant strategies and stakers
staker_strategy_weights_sum AS (
  SELECT *,
    SUM(staker_strategy_weight) OVER (PARTITION BY staker, reward_hash, snapshot) as staker_total_strategy_weight
  FROM staker_strategy_weights
),
-- Calculate staker strategy proportion of tokens for each reward and snapshot
staker_strategy_proportions AS (
  SELECT *,
    FLOOR((staker_strategy_weight / staker_total_strategy_weight) * 1000000000000000) / 1000000000000000 as staker_strategy_proportion
  FROM staker_strategy_weights_sum
),
staker_strategy_tokens AS (
  SELECT *,
    floor(staker_strategy_proportion * staker_tokens) as staker_strategy_tokens
  FROM staker_strategy_proportions
)
SELECT * from staker_strategy_tokens
 
`

type RfaeStakerStrategyPayout struct {
	RewardHash                string
	Snapshot                  time.Time
	Token                     string
	TokensPerDay              float64
	Avs                       string
	Strategy                  string
	Multiplier                string
	RewardType                string
	Operator                  string
	Staker                    string
	Shares                    string
	StakerTokens              string
	StakerStrategyWeight      string
	StakerTotalStrategyWeight string
	StakerStrategyProportion  string
	StakerStrategyTokens      string
}

func (osr *RfaeStakerStrategyPayout) TableName() string {
	return "sot_4_rfae_staker_strategy_payout"
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert4RfaeStakerStrategyPayout(cutoffDate string) error {
	sog.logger.Sugar().Infow("Generating and inserting 4_rfaeStakerStrategyPayout",
		"cutoffDate", cutoffDate,
	)
	tableName := "sot_4_rfae_staker_strategy_payout"
	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)

	query, err := rewardsUtils.RenderQueryTemplate(_4_rfaeStakerStrategyPayoutsQuery, map[string]interface{}{
		"activeRewardsTable": allTableNames[rewardsUtils.Table_1_ActiveRewards],
		"rfaeStakerTable":    allTableNames[rewardsUtils.Table_5_RfaeStakers],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 4_rfaeStakerStrategyPayoutsQuery query", "error", err)
		return err
	}

	err = rewardsUtils.GenerateAndInsertFromQuery(sog.db, tableName, query, nil, sog.logger)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to generate 4_rfaeStakerStrategyPayoutsQuery", "error", err)
		return err
	}
	return nil
}

func (sog *StakerOperatorsGenerator) List4RfaeStakerStrategyPayout() ([]*RfaeStakerStrategyPayout, error) {
	var rewards []*RfaeStakerStrategyPayout
	res := sog.db.Model(&RfaeStakerStrategyPayout{}).Find(&rewards)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to list 4_rfaeStakerStrategyPayoutsQuery", "error", res.Error)
		return nil, res.Error
	}
	return rewards, nil
}
