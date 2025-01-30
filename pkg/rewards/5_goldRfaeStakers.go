package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _5_goldRfaeStakersQuery = `
create table {{.destTableName}} as
WITH combined_operators AS (
  SELECT DISTINCT
    snapshot,
    operator
  FROM (
    -- Always include AVS operators
    SELECT snapshot, operator FROM operator_avs_registration_snapshots
    UNION
    -- Include operator set operators only after Mississippi hard fork
    SELECT 
      snapshot, 
      operator 
    FROM operator_set_operator_registration_snapshots
    WHERE snapshot >= @mississippiForkDate
  ) all_operators
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
    co.operator
  FROM {{.activeRewardsTable}} ap
  JOIN combined_operators co
  ON ap.snapshot = co.snapshot
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
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  WHERE sss.shares > 0 and sdo.multiplier != 0
),
addresses_to_exclude AS (
    select address as excluded_address from excluded_addresses where network = @network 
),
-- Parse out the stakers who are addresses
parsed_out_excluded_addresses AS (
  SELECT * from staker_strategy_shares sss
  LEFT JOIN addresses_to_exclude ate ON sss.staker = ate.excluded_address
    WHERE 
      -- The end result here is that null excluded addresses are not selected UNLESS after the cutoff date
      ate.excluded_address IS NULL  -- Earner is not in the exclusion list
      OR sss.snapshot >= DATE(@panamaForkDate)  -- Or snapshot is on or after the cutoff date
),
-- Calculate the weight of a staker
staker_weights AS (
  SELECT *,
    SUM(multiplier * shares) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
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
    SUM(staker_weight) OVER (PARTITION BY reward_hash, snapshot) as total_weight
  FROM distinct_stakers
),
-- Calculate staker proportion of tokens for each reward and snapshot
staker_proportion AS (
  SELECT *,
    FLOOR((staker_weight / total_weight) * 1000000000000000) / 1000000000000000 AS staker_proportion
  FROM staker_weight_sum
),
-- Calculate total tokens to the (staker, operator) pair
staker_operator_total_tokens AS (
  SELECT *,
    FLOOR(staker_proportion * tokens_per_day_decimal) as total_staker_operator_payout
  FROM staker_proportion
),
-- Calculate the token breakdown for each (staker, operator) pair with dynamic split logic
-- If no split is found, default to 1000 (10%)
token_breakdowns AS (
  SELECT sott.*,
    CASE
      WHEN sott.snapshot < @arnoHardforkDate AND sott.reward_submission_date < @arnoHardforkDate THEN
        floor(sott.total_staker_operator_payout * 0.10)
      WHEN sott.snapshot < @trinityHardforkDate AND sott.reward_submission_date < @trinityHardforkDate THEN 
        floor(sott.total_staker_operator_payout * COALESCE(ops.split, 1000) / CAST(10000 AS DECIMAL))
      ELSE
        floor(sott.total_staker_operator_payout * COALESCE(ops.split, dos.split, 1000) / CAST(10000 AS DECIMAL))
    END as operator_tokens,
    CASE
      WHEN sott.snapshot < @arnoHardforkDate AND sott.reward_submission_date < @arnoHardforkDate THEN
        sott.total_staker_operator_payout - floor(sott.total_staker_operator_payout * 0.10)
      WHEN sott.snapshot < @trinityHardforkDate AND sott.reward_submission_date < @trinityHardforkDate THEN 
        sott.total_staker_operator_payout - floor(sott.total_staker_operator_payout * COALESCE(ops.split, 1000) / CAST(10000 AS DECIMAL))
      ELSE
        sott.total_staker_operator_payout - floor(sott.total_staker_operator_payout * COALESCE(ops.split, dos.split, 1000) / CAST(10000 AS DECIMAL))
    END as staker_tokens
  FROM staker_operator_total_tokens sott
  LEFT JOIN operator_pi_split_snapshots ops
  ON sott.operator = ops.operator AND sott.snapshot = ops.snapshot
  LEFT JOIN default_operator_split_snapshots dos ON (sott.snapshot = dos.snapshot)
)
SELECT * from token_breakdowns
ORDER BY reward_hash, snapshot, staker, operator
`

func (rc *RewardsCalculator) GenerateGold5RfaeStakersTable(snapshotDate string, forks config.ForkMap) error {
	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_5_RfaeStakers]

	rc.logger.Sugar().Infow("Generating rfae stakers table",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
		zap.String("arnoHardforkDate", forks[config.RewardsFork_Arno]),
		zap.String("trinityHardforkDate", forks[config.RewardsFork_Trinity]),
		zap.String("mississippiForkDate", forks[config.RewardsFork_Mississippi]),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_5_goldRfaeStakersQuery, map[string]interface{}{
		"destTableName":      destTableName,
		"activeRewardsTable": allTableNames[rewardsUtils.Table_1_ActiveRewards],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query,
		sql.Named("panamaForkDate", forks[config.RewardsFork_Panama]),
		sql.Named("network", rc.globalConfig.Chain.String()),
		sql.Named("arnoHardforkDate", forks[config.RewardsFork_Arno]),
		sql.Named("trinityHardforkDate", forks[config.RewardsFork_Trinity]),
		sql.Named("mississippiForkDate", forks[config.RewardsFork_Mississippi]),
	)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to generate gold_rfae_stakers", "error", res.Error)
		return res.Error
	}
	return nil
}
