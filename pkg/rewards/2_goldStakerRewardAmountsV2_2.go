package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _2_goldStakerRewardAmountsV2_2Query = `
create table {{.destTableName}} as
WITH reward_snapshot_operators as (
  SELECT
    ap.reward_hash,
    ap.snapshot as snapshot,
    ap.token,
    ap.tokens_per_day,
    ap.tokens_per_day_decimal,
    ap.avs,
    ap.strategy,
    ap.multiplier,
    ap.reward_type,
    ap.reward_submission_date,
    oar.operator
  FROM {{.activeRewardsTable}} ap
  JOIN operator_avs_registration_snapshots oar
  ON ap.avs = oar.avs and ap.snapshot = oar.snapshot
  WHERE ap.reward_type = 'avs'
),
_operator_restaked_strategies AS (
  SELECT
    rso.*
  FROM reward_snapshot_operators rso
  JOIN operator_avs_strategy_snapshots oas
  ON
    rso.operator = oas.operator AND
    rso.avs = oas.avs AND
    rso.strategy = oas.strategy AND
    rso.snapshot = oas.snapshot
),
-- V2.2: Get operator allocations for unique stake calculation
operator_unique_allocations AS (
  SELECT
    ors.*,
    oas.magnitude as unique_stake_allocation
  FROM _operator_restaked_strategies ors
  JOIN {{.operatorAllocationSnapshotsTable}} oas
  ON
    ors.operator = oas.operator AND
    ors.avs = oas.avs AND
    ors.strategy = oas.strategy AND
    ors.snapshot = oas.snapshot
  WHERE oas.magnitude > 0  -- Only consider active unique stake allocations
),
-- Get the stakers that were delegated to the operator for the snapshot
staker_delegated_operators AS (
  SELECT
    oua.*,
    sds.staker
  FROM operator_unique_allocations oua
  JOIN staker_delegation_snapshots sds
  ON
    oua.operator = sds.operator AND
    oua.snapshot = sds.snapshot
),
-- V2.2: Calculate staker proportions based on their total delegated shares to the operator
-- but only use unique stake allocations for the reward calculations
staker_avs_strategy_shares AS (
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
-- Calculate staker weights based on their shares (for proportion calculation)
staker_weights AS (
  SELECT *,
    SUM(multiplier * shares) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
  FROM staker_avs_strategy_shares
),
-- Get distinct stakers since their weights are already calculated
distinct_stakers AS (
  SELECT *
  FROM (
      SELECT *,
        ROW_NUMBER() OVER (PARTITION BY reward_hash, snapshot, staker ORDER BY strategy ASC) as rn
      FROM staker_weights
  ) t
  WHERE rn = 1
  ORDER BY reward_hash, snapshot, staker
),
-- V2.2: Calculate sum of all staker weights for each reward and snapshot, scoped to unique stake
staker_weight_sum AS (
  SELECT *,
    SUM(staker_weight) OVER (PARTITION BY reward_hash, snapshot) as total_staker_weight,
    -- V2.2: Use unique stake allocation instead of total tokens per day
    SUM(CAST(unique_stake_allocation AS DECIMAL(38,0))) OVER (PARTITION BY reward_hash, snapshot) as total_unique_stake_allocation
  FROM distinct_stakers
),
-- Calculate staker proportion of the unique stake allocation
staker_proportion AS (
  SELECT *,
    FLOOR((staker_weight / total_staker_weight) * 1000000000000000) / 1000000000000000 AS staker_proportion
  FROM staker_weight_sum
),
-- V2.2: Calculate total tokens to the (staker, operator) pair based on unique stake
staker_operator_total_tokens AS (
  SELECT *,
    CASE
      -- For snapshots that are at or after the v2.2 fork, use unique stake calculation
      WHEN snapshot >= @pecosForkDate THEN
        FLOOR(staker_proportion * CAST(unique_stake_allocation AS DECIMAL(38,0)) * tokens_per_day_decimal / total_unique_stake_allocation)
      -- For snapshots before the hard fork, fall back to old calc method
      WHEN snapshot < @amazonHardforkDate AND reward_submission_date < @amazonHardforkDate THEN
        cast(staker_proportion * tokens_per_day AS DECIMAL(38,0))
      WHEN snapshot < @nileHardforkDate AND reward_submission_date < @nileHardforkDate THEN
        (staker_proportion * tokens_per_day)::text::decimal(38,0)
      ELSE
        FLOOR(staker_proportion * tokens_per_day_decimal)
    END as total_staker_operator_payout
  FROM staker_proportion
),
-- Calculate the token breakdown for each (staker, operator) pair with dynamic split logic
token_breakdowns AS (
  SELECT sott.*,
    CASE
      WHEN sott.snapshot < @amazonHardforkDate AND sott.reward_submission_date < @amazonHardforkDate THEN
        cast(sott.total_staker_operator_payout * 0.10 AS DECIMAL(38,0))
      WHEN sott.snapshot < @nileHardforkDate AND sott.reward_submission_date < @nileHardforkDate THEN
        (sott.total_staker_operator_payout * 0.10)::text::decimal(38,0)
      WHEN sott.snapshot < @arnoHardforkDate AND sott.reward_submission_date < @arnoHardforkDate THEN
        floor(sott.total_staker_operator_payout * 0.10)
      WHEN sott.snapshot < @trinityHardforkDate AND sott.reward_submission_date < @trinityHardforkDate THEN
        floor(sott.total_staker_operator_payout * COALESCE(oas.split, 1000) / CAST(10000 AS DECIMAL))
      ELSE
        floor(sott.total_staker_operator_payout * COALESCE(oas.split, dos.split, 1000) / CAST(10000 AS DECIMAL))
    END as operator_tokens,
    CASE
      WHEN sott.snapshot < @amazonHardforkDate AND sott.reward_submission_date < @amazonHardforkDate THEN
        sott.total_staker_operator_payout - cast(sott.total_staker_operator_payout * 0.10 as DECIMAL(38,0))
      WHEN sott.snapshot < @nileHardforkDate AND sott.reward_submission_date < @nileHardforkDate THEN
        sott.total_staker_operator_payout - ((sott.total_staker_operator_payout * 0.10)::text::decimal(38,0))
      WHEN sott.snapshot < @arnoHardforkDate AND sott.reward_submission_date < @arnoHardforkDate THEN
        sott.total_staker_operator_payout - floor(sott.total_staker_operator_payout * 0.10)
      WHEN sott.snapshot < @trinityHardforkDate AND sott.reward_submission_date < @trinityHardforkDate THEN
        sott.total_staker_operator_payout - floor(sott.total_staker_operator_payout * COALESCE(oas.split, 1000) / CAST(10000 AS DECIMAL))
      ELSE
        sott.total_staker_operator_payout - floor(sott.total_staker_operator_payout * COALESCE(oas.split, dos.split, 1000) / CAST(10000 AS DECIMAL))
    END as staker_tokens
  FROM staker_operator_total_tokens sott
  LEFT JOIN operator_avs_split_snapshots oas
  ON sott.operator = oas.operator AND sott.avs = oas.avs AND sott.snapshot = oas.snapshot
  LEFT JOIN default_operator_split_snapshots dos ON (sott.snapshot = dos.snapshot)
)
SELECT * from token_breakdowns
ORDER BY reward_hash, snapshot, staker, operator
`

func (rc *RewardsCalculator) GenerateGold2StakerRewardAmountsV2_2Table(snapshotDate string, forks *config.ForkMap) error {
	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_2_StakerRewardAmounts]
	activeRewardsTable := allTableNames[rewardsUtils.Table_1_ActiveRewards]
	operatorAllocationSnapshotsTable := allTableNames[rewardsUtils.Table_OperatorAllocationSnapshots]

	rc.logger.Sugar().Infow("Generating v2.2 staker reward amounts",
		zap.String("snapshotDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_2_goldStakerRewardAmountsV2_2Query, map[string]interface{}{
		"destTableName":                    destTableName,
		"activeRewardsTable":               activeRewardsTable,
		"operatorAllocationSnapshotsTable": operatorAllocationSnapshotsTable,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render v2.2 query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query,
		sql.Named("amazonHardforkDate", forks[config.RewardsFork_Amazon].Date),
		sql.Named("nileHardforkDate", forks[config.RewardsFork_Nile].Date),
		sql.Named("arnoHardforkDate", forks[config.RewardsFork_Arno].Date),
		sql.Named("trinityHardforkDate", forks[config.RewardsFork_Trinity].Date),
		sql.Named("pecosForkDate", forks[config.RewardsFork_Pecos].Date),
	)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to generate v2.2 staker reward amounts", "error", res.Error)
		return res.Error
	}

	return nil
}
