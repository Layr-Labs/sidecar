package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _3_goldOperatorRewardAmountsQuery = `
INSERT INTO {{.destTableName}} (reward_hash, snapshot, token, tokens_per_day, avs, strategy, multiplier, reward_type, operator, operator_tokens, rn, generated_rewards_snapshot_id) as
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
SELECT *, @generatedRewardsSnapshotId as generated_rewards_snapshot_id FROM distinct_operators
`

func (rc *RewardsCalculator) GenerateGold3OperatorRewardAmountsTable(snapshotDate string, generatedSnapshotId uint64) error {
	destTableName := rewardsUtils.RewardsTable_3_OperatorRewardAmounts

	rc.logger.Sugar().Infow("Generating operator reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_3_goldOperatorRewardAmountsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"stakerRewardAmountsTable":   rewardsUtils.RewardsTable_2_StakerRewardAmounts,
		"generatedRewardsSnapshotId": generatedSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	// Add ON CONFLICT clause to the query
	query = query + " ON CONFLICT (reward_hash, avs, operator, strategy, snapshot) DO NOTHING"

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_operator_reward_amounts", "error", res.Error)
		return res.Error
	}
	return nil
}
