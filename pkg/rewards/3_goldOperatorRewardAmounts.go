package rewards

import (
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _3_goldOperatorRewardAmountsQuery = `
create table {{.destTableName}} as
WITH operator_token_sums AS (
  SELECT
    reward_hash,
    snapshot,
    token,
    tokens_per_day,
    avs,
    strategy,
    multiplier,
    reward_type,
    operator,
    SUM(operator_tokens) OVER (PARTITION BY operator, reward_hash, snapshot) AS operator_tokens
  FROM {{.stakerRewardAmountsTable}}
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

func (rc *RewardsCalculator) GenerateGold3OperatorRewardAmountsTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	destTableName := rc.getTempOperatorRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId)
	stakerRewardAmountsTable := rc.getTempStakerRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId)

	if err := rc.DropTempOperatorRewardAmountsTable(snapshotDate, generatedRewardsSnapshotId); err != nil {
		rc.logger.Sugar().Errorw("Failed to drop existing temp operator reward amounts table", "error", err)
		return err
	}

	rc.logger.Sugar().Infow("Generating operator reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_3_goldOperatorRewardAmountsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"stakerRewardAmountsTable":   stakerRewardAmountsTable,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create temp operator reward amounts", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) getTempOperatorRewardAmountsTableName(snapshotDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(snapshotDate)
	return fmt.Sprintf("tmp_rewards_gold_3_operator_reward_amounts_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (rc *RewardsCalculator) DropTempOperatorRewardAmountsTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := rc.getTempOperatorRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to drop temp operator reward amounts table", "error", res.Error)
		return res.Error
	}
	rc.logger.Sugar().Infow("Successfully dropped temp operator reward amounts table",
		zap.String("tempTableName", tempTableName),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	return nil
}
