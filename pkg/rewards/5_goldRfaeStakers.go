package rewards

const _5_goldRfaeStakersQuery = `
insert into gold_5_rfae_stakers
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
    ap.tokens_per_day_decimal,
    ap.avs,
    ap.strategy,
    ap.multiplier,
    ap.reward_type,
    ap.reward_submission_date,
    aoo.operator
  FROM gold_1_active_rewards ap
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
-- Calculate the weight of a staker
staker_weights_grouped as (
	SELECT
		staker,
		reward_hash,
		snapshot,
		sum_big(staker_weight(multiplier, shares)) as staker_weight
	from staker_strategy_shares
	group by staker, reward_hash, snapshot
),
staker_weights AS (
  SELECT
      sss.*,
      swg.staker_weight
  FROM staker_strategy_shares as sss
  join staker_weights_grouped as swg on (
	sss.staker = swg.staker
	and sss.reward_hash = swg.reward_hash
   	and sss.snapshot = swg.snapshot
  )
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
staker_weight_sum_groups as (
	SELECT
		reward_hash,
		snapshot,
		sum_big(staker_weight) as total_weight
	FROM distinct_stakers
	GROUP BY reward_hash, snapshot
),
staker_weight_sum AS (
  SELECT
      ds.*,
      swsg.total_weight
  FROM distinct_stakers as ds
  JOIN staker_weight_sum_groups as swsg on (
      ds.reward_hash = swsg.reward_hash
      and ds.snapshot = swsg.snapshot
  )
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

func (rc *RewardsCalculator) GenerateGold5RfaeStakersTable() error {
	res := rc.grm.Exec(_5_goldRfaeStakersQuery)
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
