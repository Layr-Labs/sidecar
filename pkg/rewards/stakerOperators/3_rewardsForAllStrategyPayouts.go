package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"time"
)

const _3_rewardsForAllStrategyPayoutsQuery = `
create table {{.destTableName}} as
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
  JOIN staker_share_snapshots as sss
  ON ap.strategy = sss.strategy and ap.snapshot = sss.snapshot
  WHERE ap.reward_type = 'all_stakers'
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  AND sss.shares > 0 and ap.multiplier != 0
),
-- Calculate the weight of a staker
staker_weights AS (
  SELECT *,
    multiplier * shares AS staker_weight
  FROM reward_snapshot_stakers
),
-- Calculate sum of all staker weights
staker_weight_sum AS (
  SELECT *,
    SUM(staker_weight) OVER (PARTITION BY staker, reward_hash, snapshot) as total_staker_weight
  FROM staker_weights
),
-- Calculate staker token proportion
staker_proportion AS (
  SELECT *,
    FLOOR((staker_weight / total_staker_weight) * 1000000000000000) / 1000000000000000 AS staker_proportion
  FROM staker_weight_sum
),
-- Calculate total tokens to staker
staker_tokens AS (
  SELECT *,
  -- TODO: update to using floor when we reactivate this
  (tokens_per_day * staker_proportion)::text::decimal(38,0) as staker_strategy_tokens
  FROM staker_proportion
)
SELECT * from staker_tokens
`

type RewardsForAllStrategyPayout struct {
	RewardHash           string
	Snapshot             time.Time
	Token                string
	TokensPerDay         float64
	Avs                  string
	Strategy             string
	Multiplier           string
	RewardType           string
	Staker               string
	Shares               string
	StakerStrategyTokens string
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert3RewardsForAllStrategyPayout(cutoffDate string) error {
	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_3_RewardsForAllStrategyPayout]

	sog.logger.Sugar().Infow("Generating and inserting 3_rewardsForAllStrategyPayouts",
		"cutoffDate", cutoffDate,
	)

	query, err := rewardsUtils.RenderQueryTemplate(_3_rewardsForAllStrategyPayoutsQuery, map[string]interface{}{
		"destTableName":      destTableName,
		"activeRewardsTable": allTableNames[rewardsUtils.Table_1_ActiveRewards],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 3_rewardsForAllStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 3_rewardsForAllStrategyPayouts", "error", res.Error)
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
