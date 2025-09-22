package stakerOperators

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _1_stakerStrategyPayoutsQuery = `
create table {{.destTableName}} as
WITH reward_snapshot_operators as (
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
    oar.operator,
    ap.reward_submission_date
  FROM {{.activeRewardsTable}} ap
  JOIN operator_avs_registration_snapshots oar
  ON ap.avs = oar.avs and ap.snapshot = oar.snapshot
  WHERE ap.reward_type = 'avs'
),
-- Get the strategies that the operator is restaking on the snapshot
operator_restaked_strategies AS (
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
    sdo.staker = sss.staker AND
    sdo.snapshot = sss.snapshot AND
    sdo.strategy = sss.strategy
),
-- Join the strategies that were not included in staker_rewards originally
rejoined_staker_strategies AS (
  SELECT
    sas.*,
    spa.staker_tokens
  FROM staker_avs_strategy_shares sas
  JOIN {{.stakerRewardAmountsTable}} spa
  ON
    sas.snapshot = spa.snapshot AND
    sas.reward_hash = spa.reward_hash AND
    sas.staker = spa.staker
  WHERE sas.shares > 0 AND sas.multiplier != 0
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
staker_operator_total_tokens AS (
  SELECT *,
    CASE
      -- For snapshots that are before the hard fork AND submitted before the hard fork, we use the old calc method
      WHEN snapshot < @amazonHardforkDate AND reward_submission_date < @amazonHardforkDate THEN
        cast(staker_strategy_proportion * staker_tokens AS DECIMAL(38,0))
      WHEN snapshot < @nileHardforkDate AND reward_submission_date < @nileHardforkDate THEN
        (staker_strategy_proportion * staker_tokens)::text::decimal(38,0)
      ELSE
        FLOOR(staker_strategy_proportion * staker_tokens)
    END as staker_strategy_tokens
  FROM staker_strategy_proportions
)
select * from staker_operator_total_tokens
`

type StakerStrategyPayout struct {
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

func (sog *StakerOperatorsGenerator) GenerateAndInsert1StakerStrategyPayouts(cutoffDate string, forks config.ForkMap, generatedRewardsSnapshotId uint64) error {
	destTableName := sog.getTempStakerStrategyPayoutsTableName(cutoffDate, generatedRewardsSnapshotId)

	sog.logger.Sugar().Infow("Generating temp 1_stakerStrategyPayouts",
		"cutoffDate", cutoffDate,
		"destTableName", destTableName,
	)

	// Drop existing temp table
	if err := sog.DropTempStakerStrategyPayoutsTable(cutoffDate, generatedRewardsSnapshotId); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop existing temp staker strategy payouts table", "error", err)
		return err
	}

	// Use temp tables from gold rewards
	tempActiveRewardsTable := sog.getTempActiveRewardsTableName(cutoffDate, generatedRewardsSnapshotId)
	tempStakerRewardAmountsTable := sog.getTempStakerRewardAmountsTableName(cutoffDate, generatedRewardsSnapshotId)

	query, err := rewardsUtils.RenderQueryTemplate(_1_stakerStrategyPayoutsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"activeRewardsTable":         tempActiveRewardsTable,
		"stakerRewardAmountsTable":   tempStakerRewardAmountsTable,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 1_stakerStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query,
		sql.Named("amazonHardforkDate", forks[config.RewardsFork_Amazon].Date),
		sql.Named("nileHardforkDate", forks[config.RewardsFork_Nile].Date),
	)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate temp 1_stakerStrategyPayouts", "error", res.Error)
		return res.Error
	}
	return nil
}

func (sog *StakerOperatorsGenerator) getTempStakerStrategyPayoutsTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_staker_operators_1_staker_strategy_payouts_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (sog *StakerOperatorsGenerator) DropTempStakerStrategyPayoutsTable(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := sog.getTempStakerStrategyPayoutsTableName(cutoffDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := sog.db.Exec(query)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to drop temp staker strategy payouts table", "error", res.Error)
		return res.Error
	}
	sog.logger.Sugar().Infow("Successfully dropped temp staker strategy payouts table",
		zap.String("tempTableName", tempTableName),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	return nil
}

func (rc *StakerOperatorsGenerator) getTempActiveRewardsTableName(snapshotDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(snapshotDate)
	return fmt.Sprintf("tmp_rewards_gold_1_active_rewards_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (sog *StakerOperatorsGenerator) getTempStakerRewardAmountsTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_rewards_gold_2_staker_reward_amounts_%s_%d", camelDate, generatedRewardSnapshotId)
}
