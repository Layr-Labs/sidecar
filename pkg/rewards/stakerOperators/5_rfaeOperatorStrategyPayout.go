package stakerOperators

import (
	"fmt"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _5_rfaeOperatorStrategyPayoutsQuery = `
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
operator_strategy_shares AS (
  SELECT
    rso.*,
    oss.shares
  FROM reward_snapshot_operators rso
  JOIN operator_share_snapshots oss
  ON
    rso.operator = oss.operator AND
    rso.strategy = oss.strategy AND
    rso.snapshot = oss.snapshot
),
rejoined_operator_strategies AS (
  SELECT
    oss.*,
    rfao.operator_tokens
  FROM operator_strategy_shares oss
  JOIN {{.rfaeOperatorsTable}} rfao
  ON
    oss.snapshot = rfao.snapshot AND
    oss.reward_hash = rfao.reward_hash AND
    oss.operator = rfao.operator
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  WHERE oss.shares > 0 and oss.multiplier > 0
),
-- Calculate the weight of a operator for each of their strategies
operator_strategy_weights AS (
  SELECT *,
    multiplier * shares AS operator_strategy_weight
  FROM rejoined_operator_strategies
  ORDER BY reward_hash, snapshot, operator, strategy
),
-- Calculate sum of each operator operator_strategy_weight for each reward and snapshot for a given operator
operator_strategy_weights_sum AS (
  SELECT *,
    SUM(operator_strategy_weight) OVER (PARTITION BY operator, reward_hash, snapshot) as operator_total_strategy_weight
  FROM operator_strategy_weights
),
-- Calculate operator strategy proportion of tokens for each reward and snapshot
operator_strategy_proportions AS (
  SELECT *,
    FLOOR((operator_strategy_weight / operator_total_strategy_weight) * 1000000000000000) / 1000000000000000 as operator_strategy_proportion
  FROM operator_strategy_weights_sum
),
operator_strategy_tokens AS (
  SELECT *,
    floor(operator_strategy_proportion * operator_tokens) as operator_strategy_tokens
  FROM operator_strategy_proportions
)
SELECT * FROM operator_strategy_tokens
`

type RfaeOperatorStrategyPayout struct {
	RewardHash             string
	Snapshot               time.Time
	Token                  string
	TokensPerDay           float64
	Avs                    string
	Strategy               string
	Multiplier             string
	RewardType             string
	Operator               string
	Shares                 string
	OperatorStrategyTokens string
}

func (sog *StakerOperatorsGenerator) GenerateAndInsert5RfaeOperatorStrategyPayout(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	destTableName := sog.getTempRfaeOperatorStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	sog.logger.Sugar().Infow("Generating temp 5_rfaeOperatorStrategyPayouts",
		"cutoffDate", cutoffDate,
		"destTableName", destTableName,
	)

	// Drop existing temp table
	if err := sog.DropTempRfaeOperatorStrategyPayoutTable(cutoffDate, generatedRewardsSnapshotId); err != nil {
		sog.logger.Sugar().Errorw("Failed to drop existing temp rfae operator strategy payout table", "error", err)
		return err
	}

	// Use temp tables from gold rewards
	tempActiveRewardsTable := sog.getTempActiveRewardsTableName(cutoffDate, generatedRewardsSnapshotId)
	tempRfaeOperatorsTable := sog.getTempRfaeOperatorsTableName(cutoffDate, generatedRewardsSnapshotId)

	query, err := rewardsUtils.RenderQueryTemplate(_5_rfaeOperatorStrategyPayoutsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"activeRewardsTable":         tempActiveRewardsTable,
		"rfaeOperatorsTable":         tempRfaeOperatorsTable,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		sog.logger.Sugar().Errorw("Failed to render 5_rfaeOperatorStrategyPayouts query", "error", err)
		return err
	}

	res := sog.db.Exec(query)

	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to generate temp 5_rfaeOperatorStrategyPayouts", "error", res.Error)
		return res.Error
	}
	return nil
}

func (sog *StakerOperatorsGenerator) getTempRfaeOperatorStrategyPayoutTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_staker_operators_5_rfae_operator_strategy_payout_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (sog *StakerOperatorsGenerator) DropTempRfaeOperatorStrategyPayoutTable(cutoffDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := sog.getTempRfaeOperatorStrategyPayoutTableName(cutoffDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := sog.db.Exec(query)
	if res.Error != nil {
		sog.logger.Sugar().Errorw("Failed to drop temp rfae operator strategy payout table", "error", res.Error)
		return res.Error
	}
	sog.logger.Sugar().Infow("Successfully dropped temp rfae operator strategy payout table",
		zap.String("tempTableName", tempTableName),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	return nil
}

func (sog *StakerOperatorsGenerator) getTempRfaeOperatorsTableName(cutoffDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(cutoffDate)
	return fmt.Sprintf("tmp_rewards_gold_6_rfae_operators_%s_%d", camelDate, generatedRewardSnapshotId)
}
