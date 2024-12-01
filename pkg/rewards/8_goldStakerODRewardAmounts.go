package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"go.uber.org/zap"
)

const _8_goldStakerODRewardAmountsQuery = `
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
-- Get the stakers that were delegated to the operator for the snapshot
staker_delegated_operators AS (
  SELECT
    ors.*,
    sds.staker
  FROM _operator_restaked_strategies ors
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
    sdo.staker = sss.staker AND
    sdo.snapshot = sss.snapshot AND
    sdo.strategy = sss.strategy
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  WHERE sss.shares > 0 and sdo.multiplier != 0
),
-- Calculate the weight of a staker
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
    CASE
      -- For snapshots that are before the hard fork AND submitted before the hard fork, we use the old calc method
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
-- If no split is found, default to 1000 (10%)
token_breakdowns AS (
  SELECT sott.*,
    CASE
      WHEN sott.snapshot < @amazonHardforkDate AND sott.reward_submission_date < @amazonHardforkDate THEN
        cast(sott.total_staker_operator_payout * COALESCE(oas.split, 1000) / 10000.0 AS DECIMAL(38,0))
      WHEN sott.snapshot < @nileHardforkDate AND sott.reward_submission_date < @nileHardforkDate THEN
        (sott.total_staker_operator_payout * COALESCE(oas.split, 1000) / 10000.0)::text::decimal(38,0)
      ELSE
        floor(sott.total_staker_operator_payout * COALESCE(oas.split, 1000) / 10000.0)
    END as operator_tokens,
    CASE
      WHEN sott.snapshot < @amazonHardforkDate AND sott.reward_submission_date < @amazonHardforkDate THEN
        sott.total_staker_operator_payout - cast(sott.total_staker_operator_payout * COALESCE(oas.split, 1000) / 10000.0 AS DECIMAL(38,0))
      WHEN sott.snapshot < @nileHardforkDate AND sott.reward_submission_date < @nileHardforkDate THEN
        sott.total_staker_operator_payout - ((sott.total_staker_operator_payout * COALESCE(oas.split, 1000) / 10000.0)::text::decimal(38,0))
      ELSE
        sott.total_staker_operator_payout - floor(sott.total_staker_operator_payout * COALESCE(oas.split, 1000) / 10000.0)
    END as staker_tokens
  FROM staker_operator_total_tokens sott
  LEFT JOIN operator_avs_split_snapshots oas
  ON sott.operator = oas.operator AND sott.avs = oas.avs AND sott.snapshot = oas.snapshot
)
SELECT * from token_breakdowns
ORDER BY reward_hash, snapshot, staker, operator
`

func (rc *RewardsCalculator) GenerateGold8StakerODRewardAmountsTable(snapshotDate string, forks config.ForkMap) error {
	allTableNames := getGoldTableNames(snapshotDate)
	destTableName := allTableNames[Table_8_StakerODRewardAmounts]

	rc.logger.Sugar().Infow("Generating staker OD reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
		zap.String("amazonHardforkDate", forks[config.Fork_Amazon]),
		zap.String("nileHardforkDate", forks[config.Fork_Nile]),
	)

	query, err := renderQueryTemplate(_8_goldStakerODRewardAmountsQuery, map[string]string{
		"destTableName":      destTableName,
		"activeRewardsTable": allTableNames[Table_7_ActiveODRewards],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query,
		sql.Named("amazonHardforkDate", forks[config.Fork_Amazon]),
		sql.Named("nileHardforkDate", forks[config.Fork_Nile]),
	)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_staker_od_reward_amounts", "error", res.Error)
		return res.Error
	}
	return nil
}
