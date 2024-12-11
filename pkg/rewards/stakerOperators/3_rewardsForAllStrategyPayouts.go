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
const _3_rewardsForAllStrategyPayoutsQuery = `
WITH reward_snapshot_stakers AS (
  SELECT 
    ap.reward_hash,
    ap.snapshot,
    ap.token,
    ap.tokens_per_day,
    ap.avs,
    ap.strategy,
    ap.multiplier,
    ap.reward_type,
    sss.staker,
    sss.shares
  FROM {{.activeRewardsTable}} ap
  JOIN staker_share_snapshots sss
  ON ap.strategy = sss.strategy and ap.snapshot = sss.snapshot
  WHERE ap.reward_type = 'all_stakers'
),
-- Join the strategies that were not included in pay for all rewards originally
rejoined_staker_strategies AS (
  SELECT
    rss.*,
    rfa.staker_tokens
  FROM reward_snapshot_stakers rss
  JOIN {{.rewardsForAllTable}} rfa
  ON
    rss.snapshot = rfa.snapshot AND
    rss.reward_hash = rfa.reward_hash AND
    rss.staker = rfa.staker
  WHERE rss.shares > 0 and rss.multiplier != 0
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
staker_strategy_p4a_tokens AS (
  SELECT *,
    floor(staker_strategy_proportion * staker_tokens) as staker_strategy_tokens
  FROM staker_strategy_proportions
)
SELECT * from staker_strategy_p4a_tokens 
`

type RewardsForAllStrategyPayout struct {
	RewardHash                string
	Snapshot                  time.Time
	Token                     string
	TokensPerDay              float64
	Avs                       string
	Strategy                  string
	Multiplier                string
	RewardType                string
	Staker                    string
	Shares                    string
	StakerTokens              string
	StakerStrategyWeight      string
	StakerTotalStrategyWeight string
	StakerStrategyProportion  string
	StakerStrategyTokens      string
}

func (osr *RewardsForAllStrategyPayout) TableName() string {
	return "sot_3_rewards_for_all_strategy_payout"
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert3RewardsForAllStrategyPayout(cutoffDate string) error {
	sog.logger.Sugar().Infow("Generating and inserting 3_rewardsForAllStrategyPayoutsQuery",
		"cutoffDate", cutoffDate,
	)
	tableName := "sot_3_rewards_for_all_strategy_payout"
	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)

	query, err := rewardsUtils.RenderQueryTemplate(_3_rewardsForAllStrategyPayoutsQuery, map[string]interface{}{
		"activeRewardsTable": allTableNames[rewardsUtils.Table_1_ActiveRewards],
		"rewardsForAllTable": allTableNames[rewardsUtils.Table_4_RewardsForAll],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 3_rewardsForAllStrategyPayoutsQuery query", "error", err)
		return err
	}

	err = rewardsUtils.GenerateAndInsertFromQuery(sog.db, tableName, query, nil, sog.logger)
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to generate 3_rewardsForAllStrategyPayoutsQuery", "error", err)
		return err
	}
	return nil
}

func (sog *StakerOperatorsGenerator) List3RewardsForAllStrategyPayout() ([]*RewardsForAllStrategyPayout, error) {
	var rewards []*RewardsForAllStrategyPayout
	res := sog.db.Model(&RewardsForAllStrategyPayout{}).Find(&rewards)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to list 3_rewardsForAllStrategyPayoutsQuery", "error", res.Error)
		return nil, res.Error
	}
	return rewards, nil
}
