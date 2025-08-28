package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _6_goldRfaeOperatorsQuery = `
INSERT INTO {{.destTableName}} (reward_hash, snapshot, token, tokens_per_day_decimal, avs, strategy, multiplier, reward_type, operator, operator_tokens, rn, generated_rewards_snapshot_id)
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
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
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
ON CONFLICT (reward_hash, avs, operator, strategy, snapshot) DO NOTHING
`

func (rc *RewardsCalculator) GenerateGold6RfaeOperatorsTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	destTableName := rewardsUtils.RewardsTable_6_RfaeOperators

	rc.logger.Sugar().Infow("Generating rfae operators table",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_6_goldRfaeOperatorsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"rfaeStakersTable":           rewardsUtils.RewardsTable_5_RfaeStakers,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_rfae_operators", "error", res.Error)
		return res.Error
	}
	return nil
}
