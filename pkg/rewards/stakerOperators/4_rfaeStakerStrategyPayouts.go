package stakerOperators

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"time"
)

const _4_rfaeStakerStrategyPayoutsQuery = `
create table {{.destTableName}} as
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
  JOIN {{.rfaeStakersTable}} rfas
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
	RewardHash           string
	Snapshot             time.Time
	Token                string
	TokensPerDay         float64
	Avs                  string
	Strategy             string
	Multiplier           string
	RewardType           string
	Operator             string
	Staker               string
	Shares               string
	StakerStrategyTokens string
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert4RfaeStakerStrategyPayout(cutoffDate string, forks config.ForkMap) error {
	allTableNames := rewardsUtils.GetGoldTableNames(cutoffDate)
	destTableName := allTableNames[rewardsUtils.Sot_4_RfaeStakers]

	sog.logger.Sugar().Infow("Generating and inserting 4_rfaeStakerStrategyPayouts",
		"cutoffDate", cutoffDate,
	)

	if err := rewardsUtils.DropTableIfExists(sog.db, destTableName, sog.logger); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop table", "error", err)
		return err
	}

	query, err := rewardsUtils.RenderQueryTemplate(_4_rfaeStakerStrategyPayoutsQuery, map[string]interface{}{
		"destTableName":      destTableName,
		"activeRewardsTable": allTableNames[rewardsUtils.Table_1_ActiveRewards],
		"rfaeStakersTable":   allTableNames[rewardsUtils.Table_5_RfaeStakers],
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 4_rfaeStakerStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate 4_rfaeStakerStrategyPayouts", "error", res.Error)
		return err
	}
	return nil
}
