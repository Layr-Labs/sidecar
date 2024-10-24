package rewards

import (
	"database/sql"
	"github.com/Layr-Labs/go-sidecar/internal/config"
)

const _2_goldStakerRewardAmountsQuery = `
insert into gold_2_staker_reward_amounts
with big_results as (
SELECT ap.reward_hash,
        ap.snapshot,
        ap.token,
        ap.tokens_per_day,
        ap.tokens_per_day_decimal,
        ap.avs,
        ap.strategy,
        ap.multiplier,
        ap.reward_type,
        ap.reward_submission_date,
        oar.operator,
        sds.staker
    FROM gold_1_active_rewards ap
    JOIN operator_avs_registration_snapshots oar ON (ap.avs = oar.avs and ap.snapshot = oar.snapshot)
    JOIN operator_avs_strategy_snapshots oas ON (
        oar.operator = oas.operator
        and ap.avs = oas.avs
        and ap.strategy = oas.strategy
        and ap.snapshot = oas.snapshot
    )
    JOIN staker_delegation_snapshots sds ON (
        oar.operator = sds.operator AND
        ap.snapshot = sds.snapshot
    )
    WHERE
        ap.reward_type = 'avs'
    	and multiplier != '0'
    	and DATE(ap.snapshot) >= DATE(@startDate)
    	and DATE(ap.snapshot) < DATE(@cutoffDate)
),
-- This is actually reversed to what we do currently. Reason being is that sqlite's
-- query planner is bad at optimizing joins when you have one table that's absolutely
-- huge and the other that's 1-2 orders of magniutde smaller.
-- Adding the date range to select only the rows between our start and cut-off dates helps
joined_staker_shares as (
    select
        br.*,
        sss.shares
    from staker_share_snapshots sss
    join big_results br on (
        br.staker = sss.staker
        and br.snapshot = sss.snapshot
        and br.strategy = sss.strategy
    )
    where
      DATE(sss.snapshot) >= DATE(@startDate)
      and DATE(sss.snapshot) < DATE(@cutoffDate)
),
staker_weights as (
    SELECT
        *,
        sum_big(staker_weight(multiplier, shares)) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
    FROM joined_staker_shares
),
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
token_breakdowns AS (
  SELECT *,
	subtract_big(total_staker_operator_payout, operator_tokens) as staker_tokens
  FROM operator_tokens
),
final_results as (
    SELECT * from token_breakdowns
    where
        DATE(snapshot) >= DATE(@startDate)
        and DATE(snapshot) < DATE(@cutoffDate)
    ORDER BY reward_hash, snapshot, staker, operator
)
select * from final_results;
`

func (rc *RewardsCalculator) GenerateGold2StakerRewardAmountsTable(startDate string, snapshotDate string, forks config.ForkMap) error {
	res := rc.grm.Exec(_2_goldStakerRewardAmountsQuery,
		sql.Named("startDate", startDate),
		sql.Named("cutoffDate", snapshotDate),
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
