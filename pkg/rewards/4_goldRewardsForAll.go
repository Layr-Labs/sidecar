package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _4_goldRewardsForAllQuery = `
create table {{.destTableName}} as
WITH reward_snapshot_stakers AS (
  SELECT
    ap.reward_hash,
    ap.snapshot,
    ap.token,
    ap.tokens_per_day,
    ap.avs,
    ap.strategy,
    ap.multiplier,
    ap.reward_type,
    sss.staker,
    sss.shares
  FROM {{.activeRewardsTable}} ap
  JOIN staker_share_snapshots as sss
  ON ap.strategy = sss.strategy and ap.snapshot = sss.snapshot
  WHERE ap.reward_type = 'all_stakers'
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  AND sss.shares > 0 and ap.multiplier != 0
),
-- Calculate the weight of a staker
staker_weights AS (
  SELECT *,
    SUM(multiplier * shares) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
  FROM reward_snapshot_stakers
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
staker_weight_sum AS (
  SELECT *,
    SUM(staker_weight) OVER (PARTITION BY reward_hash, snapshot) as total_staker_weight
  FROM distinct_stakers
),
-- Calculate staker token proportion
staker_proportion AS (
  SELECT *,
    FLOOR((staker_weight / total_staker_weight) * 1000000000000000) / 1000000000000000 AS staker_proportion
  FROM staker_weight_sum
),
-- Calculate total tokens to staker
staker_tokens AS (
  SELECT *,
  -- TODO: update to using floor when we reactivate this
  (tokens_per_day * staker_proportion)::text::decimal(38,0) as staker_tokens
  FROM staker_proportion
)
SELECT * from staker_tokens
`

func (rc *RewardsCalculator) GenerateGold4RewardsForAllTable(snapshotDate string, generatedSnapshotId uint64) error {
	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_4_RewardsForAll]

	rc.logger.Sugar().Infow("Generating rewards for all table",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_4_goldRewardsForAllQuery, map[string]interface{}{
		"destTableName":      destTableName,
		"activeRewardsTable": allTableNames[rewardsUtils.Table_1_ActiveRewards],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_rewards_for_all", "error", res.Error)
		return res.Error
	}
	return nil
}
