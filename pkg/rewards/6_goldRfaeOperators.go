package rewards

import (
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _6_goldRfaeOperatorsQuery = `
create table {{.destTableName}} as
WITH operator_token_sums AS (
  SELECT
    reward_hash,
    snapshot,
    token,
    tokens_per_day_decimal,
    avs,
    strategy,
    multiplier,
    reward_type,
    operator,
    SUM(operator_tokens) OVER (PARTITION BY operator, reward_hash, snapshot) AS operator_tokens
  FROM {{.rfaeStakersTable}}
),
-- Dedupe the operator tokens across strategies for each operator, reward hash, and snapshot
distinct_operators AS (
  SELECT *
  FROM (
      SELECT *,
        -- We can use an arbitrary order here since the staker_weight is the same for each (operator, strategy, hash, snapshot)
        -- We use strategy ASC for better debuggability
        ROW_NUMBER() OVER (PARTITION BY reward_hash, snapshot, operator ORDER BY strategy ASC) as rn
      FROM operator_token_sums
  ) t
  WHERE rn = 1
)
SELECT *, {{.generatedRewardsSnapshotId}} as generated_rewards_snapshot_id FROM distinct_operators
`

func (rc *RewardsCalculator) GenerateGold6RfaeOperatorsTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	destTableName := rc.getTempRfaeOperatorsTableName(snapshotDate, generatedRewardsSnapshotId)
	tempRfaeStakersTable := rc.getTempRfaeStakersTableName(snapshotDate, generatedRewardsSnapshotId)

	if err := rc.DropTempRfaeOperatorsTable(snapshotDate, generatedRewardsSnapshotId); err != nil {
		rc.logger.Sugar().Errorw("Failed to drop existing temp rfae operators table", "error", err)
		return err
	}

	rc.logger.Sugar().Infow("Generating temp rfae operators table",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
		zap.String("sourceTable", tempRfaeStakersTable),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_6_goldRfaeOperatorsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"rfaeStakersTable":           tempRfaeStakersTable,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create temp rfae operators", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) getTempRfaeOperatorsTableName(snapshotDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(snapshotDate)
	return fmt.Sprintf("tmp_rewards_gold_6_rfae_operators_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (rc *RewardsCalculator) DropTempRfaeOperatorsTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := rc.getTempRfaeOperatorsTableName(snapshotDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to drop temp rfae operators table", "error", res.Error)
		return res.Error
	}
	rc.logger.Sugar().Infow("Successfully dropped temp rfae operators table",
		zap.String("tempTableName", tempTableName),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	return nil
}
