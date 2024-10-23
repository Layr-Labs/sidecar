package rewards

import (
	"database/sql"
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/config"
)

const _5_goldRfaeStakersQuery = `
insert into gold_5_rfae_stakers
WITH avs_opted_operators AS (
  SELECT DISTINCT
    snapshot,
    operator
  FROM operator_avs_registration_snapshots
),
-- Get the operators who will earn rewards for the reward submission at the given snapshot
all_earners_rewards as (
  SELECT
    ap.reward_hash,
    ap.snapshot,
    ap.token,
    ap.tokens_per_day_decimal,
    ap.avs,
    ap.strategy,
    ap.multiplier,
    ap.reward_type,
    ap.reward_submission_date
  FROM gold_1_active_rewards ap
  WHERE ap.reward_type = 'all_earners'
),
-- Get the operators who will earn rewards for the reward submission at the given snapshot
-- Optimized this to perform the filter on all_earners FIRST then do the join.
-- otherwise sqlite was thrashing and not doing anything...
reward_snapshot_operators as (
  SELECT
    aer.reward_hash,
    aer.snapshot,
    aer.token,
    aer.tokens_per_day_decimal,
    aer.avs,
    aer.strategy,
    aer.multiplier,
    aer.reward_type,
    aer.reward_submission_date,
    aoo.operator
  FROM all_earners_rewards aer
  JOIN avs_opted_operators aoo ON(aer.snapshot = aoo.snapshot)
),
-- Get the stakers that were delegated to the operator for the snapshot 
staker_delegated_operators AS (
  SELECT
    rso.*,
    sds.staker
  FROM reward_snapshot_operators rso
  JOIN staker_delegation_snapshots sds
  ON
    rso.operator = sds.operator
    and rso.snapshot = sds.snapshot
),
-- Get the shares of each strategy the staker has delegated to the operator
staker_strategy_shares AS (
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
addresses_to_exclude AS (
    SELECT
        address as excluded_address
    from excluded_addresses
    where network = @network
),
parsed_out_excluded_addresses AS (
  SELECT * from staker_strategy_shares sss
  LEFT JOIN addresses_to_exclude ate ON sss.staker = ate.excluded_address
  WHERE 
  	-- The end result here is that null excluded addresses are not selected UNLESS after the cutoff date
    ate.excluded_address IS NULL  -- Earner is not in the exclusion list
    OR sss.snapshot >= DATE(@panamaForkDate)  -- Or snapshot is on or after the cutoff date
),
staker_weights AS (
  SELECT *,
    sum_big(staker_weight(multiplier, shares)) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
  FROM parsed_out_excluded_addresses
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
    staker_token_rewards(staker_proportion, tokens_per_day_decimal) as total_staker_operator_payout
  FROM staker_proportion
),
-- Calculate the token breakdown for each (staker, operator) pair
token_breakdowns AS (
  SELECT *,
    operator_token_rewards(total_staker_operator_payout) as operator_tokens,
    subtract_big(total_staker_operator_payout, operator_token_rewards(total_staker_operator_payout)) as staker_tokens
  FROM staker_operator_total_tokens
)
SELECT * from token_breakdowns
ORDER BY reward_hash, snapshot, staker, operator
`

func (rc *RewardsCalculator) GenerateGold5RfaeStakersTable(forks config.ForkMap) error {
	fmt.Printf("Forks: %+v\n", forks[config.Fork_Panama])
	fmt.Printf("Chain: %+v\n", rc.globalConfig.Chain.String())
	res := rc.grm.Exec(_5_goldRfaeStakersQuery,
		sql.Named("panamaForkDate", forks[config.Fork_Panama]),
		sql.Named("network", rc.globalConfig.Chain.String()),
	)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to generate gold_rfae_stakers", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) CreateGold5RfaeStakersTable() error {
	query := `
		create table if not exists gold_5_rfae_stakers (
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
		rc.logger.Sugar().Errorw("Failed to create gold_5_rfae_stakers table", "error", res.Error)
		return res.Error
	}
	return nil
}
