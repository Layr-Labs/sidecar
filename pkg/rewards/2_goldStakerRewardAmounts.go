package rewards

import (
	"database/sql"
	"github.com/Layr-Labs/go-sidecar/internal/config"
)

const _2_goldStakerRewardAmountsQuery = `
insert into gold_2_staker_reward_amounts
-- sqlite filtering optimization. filter by reward_type FIRST
-- then attempt to join it with the operator_avs_registration_snapshots table
WITH filtered_reward_types as (
	SELECT
		ap.reward_hash,
		ap.snapshot,
		ap.token,
		ap.tokens_per_day,
		ap.tokens_per_day_decimal,
		ap.avs,
		ap.strategy,
		ap.multiplier,
		ap.reward_type,
		ap.reward_submission_date
	FROM gold_1_active_rewards ap
	WHERE ap.reward_type = 'avs'
),
reward_snapshot_operators as (
  SELECT
	frp.reward_hash,
	frp.snapshot,
	frp.token,
	frp.tokens_per_day,
	frp.tokens_per_day_decimal,
	frp.avs,
	frp.strategy,
	frp.multiplier,
	frp.reward_type,
	frp.reward_submission_date,
	oar.operator
  FROM filtered_reward_types frp
  JOIN operator_avs_registration_snapshots oar ON(
 	frp.avs = oar.avs
 	and frp.snapshot = oar.snapshot
  )
),
operator_restaked_strategies AS (
  SELECT
	rso.*
  FROM reward_snapshot_operators rso
  JOIN operator_avs_strategy_snapshots oas
  ON
	rso.operator = oas.operator
	and rso.avs = oas.avs
	and rso.strategy = oas.strategy
	and rso.snapshot = oas.snapshot
),
-- Get the stakers that were delegated to the operator for the snapshot
staker_delegated_operators AS (
  SELECT
	ors.*,
	sds.staker
  FROM operator_restaked_strategies ors
  JOIN staker_delegation_snapshots sds
  ON
	ors.operator = sds.operator AND
	ors.snapshot = sds.snapshot
),
-- Get the shares for staker delegated to the operator
staker_avs_strategy_shares AS (
  SELECT
	sdo.*,
	sss.shares
  FROM staker_delegated_operators sdo
  JOIN staker_share_snapshots sss
  ON
	sdo.staker = sss.staker
	and sdo.snapshot = sss.snapshot
	and sdo.strategy = sss.strategy
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  WHERE big_gt(sss.shares, '0') and sdo.multiplier != '0'
),
-- Calculate the weight of a staker
staker_weights AS (
  SELECT *,
    sum_big(staker_weight(multiplier, shares)) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
  FROM staker_avs_strategy_shares
),
-- Get distinct stakers since their weights are already calculated
distinct_stakers AS (
  SELECT *
  FROM (
	  SELECT *,
		-- We can use an arbitrary order here since the staker_weight is the same for each (staker, strategy, hash, snapshot)
		-- We use strategy ASC for better debuggability
		ROW_NUMBER() OVER (PARTITION BY reward_hash, snapshot, staker ORDER BY strategy ASC) as rn
	  FROM staker_weights
  ) t
  WHERE rn = 1
  ORDER BY reward_hash, snapshot, staker
),
-- Calculate sum of all staker weights for each reward and snapshot
staker_weight_sum AS (
  SELECT *,
    sum_big(staker_weight) OVER (PARTITION BY reward_hash, snapshot) as total_weight
  FROM distinct_stakers
),
-- Calculate staker proportion of tokens for each reward and snapshot
staker_proportion AS (
  SELECT *,
	staker_proportion(staker_weight, total_weight) as staker_proportion
  FROM staker_weight_sum
),
-- Calculate total tokens to the (staker, operator) pair
staker_operator_total_tokens AS (
  SELECT *,
	CASE -- For snapshots that are before the hard fork AND submitted before the hard fork, we use the old calc method
	  WHEN snapshot < DATE(@amazonHardforkDate) AND reward_submission_date < DATE(@amazonHardforkDate) THEN
		amazon_staker_token_rewards(staker_proportion, tokens_per_day)
	  WHEN snapshot < DATE(@nileHardforkDate) AND reward_submission_date < DATE(@nileHardforkDate) THEN
		nile_staker_token_rewards(staker_proportion, tokens_per_day)
	  ELSE
		staker_token_rewards(staker_proportion, tokens_per_day_decimal)
	END as total_staker_operator_payout
  FROM staker_proportion
),
operator_tokens as (
	select *,
		CASE
		  WHEN snapshot < DATE(@amazonHardforkDate) AND reward_submission_date < DATE(@amazonHardforkDate) THEN
			amazon_operator_token_rewards(total_staker_operator_payout)
		  WHEN snapshot < DATE(@nileHardforkDate) AND reward_submission_date < DATE(@nileHardforkDate) THEN
			nile_operator_token_rewards(total_staker_operator_payout)
		  ELSE
			operator_token_rewards(total_staker_operator_payout)
		END as operator_tokens
	from staker_operator_total_tokens
),
-- Calculate the token breakdown for each (staker, operator) pair
token_breakdowns AS (
  SELECT *,
	subtract_big(total_staker_operator_payout, operator_tokens) as staker_tokens
  FROM operator_tokens
)
SELECT * from token_breakdowns
ORDER BY reward_hash, snapshot, staker, operator
`

func (rc *RewardsCalculator) GenerateGold2StakerRewardAmountsTable(forks config.ForkMap) error {
	res := rc.grm.Exec(_2_goldStakerRewardAmountsQuery,
		sql.Named("amazonHardforkDate", forks[config.Fork_Amazon]),
		sql.Named("nileHardforkDate", forks[config.Fork_Nile]),
	)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create 2_gold_staker_reward_amounts", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) CreateGold2RewardAmountsTable() error {
	query := `
		create table if not exists gold_2_staker_reward_amounts (
			reward_hash TEXT NOT NULL,
			snapshot DATE NOT NULL,
			token TEXT NOT NULL,
			tokens_per_day TEXT NOT NULL,
			tokens_per_day_decimal TEXT NOT NULL,
			avs TEXT NOT NULL,
			strategy TEXT NOT NULL,
			multiplier TEXT NOT NULL,
			reward_type TEXT NOT NULL,
			reward_submission_date DATE NOT NULL,
			operator TEXT NOT NULL,
			staker TEXT NOT NULL,
			shares TEXT NOT NULL,
			staker_weight TEXT NOT NULL,
			rn INTEGER NOT NULL,
			total_weight TEXT NOT NULL,
			staker_proportion TEXT NOT NULL,
			total_staker_operator_payout TEXT NOT NULL,
			operator_tokens TEXT NOT NULL,
			staker_tokens TEXT NOT NULL
		)
	`
	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_2_staker_reward_amounts table", "error", res.Error)
		return res.Error
	}
	return nil
}
