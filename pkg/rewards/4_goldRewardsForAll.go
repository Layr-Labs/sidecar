package rewards

const _4_goldRewardsForAllQuery = `
insert into gold_4_rewards_for_all
WITH reward_snapshot_stakers AS (
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
    sss.staker,
    sss.shares
  FROM gold_1_active_rewards ap
  JOIN staker_share_snapshots as sss
  ON ap.strategy = sss.strategy and ap.snapshot = sss.snapshot
  WHERE
  	ap.reward_type = 'all_stakers'
  	-- Parse out negative shares and zero multiplier so there is no division by zero case
  	AND big_gt(sss.shares, '0') and ap.multiplier != '0'
),
-- Calculate the weight of a staker
staker_weights_grouped as (
	select
	  	staker,
	    reward_hash,
	    snapshot,
	    sum_big(staker_weight(multiplier, shares)) as staker_weight
	from reward_snapshot_stakers
	group by staker, reward_hash, snapshot
),
staker_weights AS (
  SELECT
      rss.*,
      swg.staker_weight
  FROM reward_snapshot_stakers as rss
  JOIN staker_weights_grouped as swg on (
	rss.staker = swg.staker
    and rss.reward_hash = swg.reward_hash
    and rss.snapshot = swg.snapshot
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
-- Calculate sum of all staker weights
staker_weight_sum_groups as (
	SELECT
		reward_hash,
		snapshot,
		sum_big(staker_weight) as total_staker_weight
	FROM distinct_stakers
	GROUP BY reward_hash, snapshot
),
staker_weight_sum AS (
	SELECT
      ds.*,
	  swsg.total_staker_weight
  	FROM distinct_stakers as ds
  	JOIN staker_weight_sum_groups as swsg on (
  		ds.reward_hash = swsg.reward_hash
  	    and ds.snapshot = swsg.snapshot
  	)
),
-- Calculate staker token proportion
staker_proportion AS (
  SELECT *,
    staker_proportion(staker_weight, total_staker_weight) as staker_proportion
  FROM staker_weight_sum
),
-- Calculate total tokens to staker
staker_tokens AS (
  SELECT *,
  -- TODO: update to using floor when we reactivate this
  nile_staker_token_rewards(staker_proportion, tokens_per_day) as staker_tokens
  -- (tokens_per_day * staker_proportion)::text::decimal(38,0) as staker_tokens
  FROM staker_proportion
)
SELECT * from staker_tokens
`

func (rc *RewardsCalculator) GenerateGold4RewardsForAllTable() error {
	res := rc.grm.Exec(_4_goldRewardsForAllQuery)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_rewards_for_all", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) CreateGold4RewardsForAllTable() error {
	query := `
		create table if not exists gold_4_rewards_for_all (
			reward_hash TEXT NOT NULL,
			snapshot DATE NOT NULL,
			token TEXT NOT NULL,
			tokens_per_day TEXT NOT NULL,
			tokens_per_day_decimal TEXT NOT NULL,
			avs TEXT NOT NULL,
			strategy TEXT NOT NULL,
			multiplier TEXT NOT NULL,
			reward_type TEXT NOT NULL,
			staker TEXT NOT NULL,
			shares TEXT NOT NULL,
			staker_weight TEXT NOT NULL,
			rn INTEGER NOT NULL,
			total_staker_weight TEXT NOT NULL,
			staker_proportion TEXT NOT NULL,
			staker_tokens TEXT NOT NULL
		)
	`
	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_4_rewards_for_all table", "error", res.Error)
		return res.Error
	}
	return nil
}
