package rewards

import (
	"time"

	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _15_goldFinalQuery = `
INSERT INTO {{.destTableName}} (earner, snapshot, reward_hash, token, amount, generated_rewards_snapshot_id)
WITH staker_rewards AS (
  -- We can select DISTINCT here because the staker's tokens are the same for each strategy in the reward hash
  SELECT DISTINCT
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amount
  FROM {{.stakerRewardAmountsTable}}
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
),
operator_rewards AS (
  SELECT DISTINCT
    -- We can select DISTINCT here because the operator's tokens are the same for each strategy in the reward hash
    operator as earner,
    snapshot,
    reward_hash,
    token,
    operator_tokens as amount
  FROM {{.operatorRewardAmountsTable}}
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
),
rewards_for_all AS (
  SELECT DISTINCT
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amount
  FROM {{.rewardsForAllTable}}
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
),
rewards_for_all_earners_stakers AS (
  SELECT DISTINCT
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amount
  FROM {{.rfaeStakerTable}}
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
),
rewards_for_all_earners_operators AS (
  SELECT DISTINCT
    operator as earner,
    snapshot,
    reward_hash,
    token,
    operator_tokens as amount
  FROM {{.rfaeOperatorTable}}
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
),
{{ if .enableRewardsV2 }}
operator_od_rewards AS (
  SELECT DISTINCT
    -- We can select DISTINCT here because the operator's tokens are the same for each strategy in the reward hash
    operator as earner,
    snapshot,
    reward_hash,
    token,
    operator_tokens as amount
  FROM {{.operatorODRewardAmountsTable}}
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
),
staker_od_rewards AS (
  SELECT DISTINCT
    -- We can select DISTINCT here because the staker's tokens are the same for each strategy in the reward hash
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amount
  FROM {{.stakerODRewardAmountsTable}}
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
),
avs_od_rewards AS (
  SELECT DISTINCT
    -- We can select DISTINCT here because the avs's tokens are the same for each strategy in the reward hash
    avs as earner,
    snapshot,
    reward_hash,
    token,
    avs_tokens as amount
  FROM {{.avsODRewardAmountsTable}}
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
),
{{ end }}
{{ if .enableRewardsV2_1 }}
operator_od_operator_set_rewards AS (
  SELECT DISTINCT
    -- We can select DISTINCT here because the operator's tokens are the same for each strategy in the reward hash
    operator as earner,
    snapshot,
    reward_hash,
    token,
    operator_tokens as amount
  FROM {{.operatorODOperatorSetRewardAmountsTable}}
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
),
staker_od_operator_set_rewards AS (
  SELECT DISTINCT
    -- We can select DISTINCT here because the staker's tokens are the same for each strategy in the reward hash
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amount
  FROM {{.stakerODOperatorSetRewardAmountsTable}}
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
),
avs_od_operator_set_rewards AS (
  SELECT DISTINCT
    -- We can select DISTINCT here because the avs's tokens are the same for each strategy in the reward hash
    avs as earner,
    snapshot,
    reward_hash,
    token,
    avs_tokens as amount
  FROM {{.avsODOperatorSetRewardAmountsTable}}
  WHERE generated_rewards_snapshot_id = {{.generatedRewardsSnapshotId}}
),
{{ end }}
combined_rewards AS (
  SELECT * FROM operator_rewards
  UNION ALL
  SELECT * FROM staker_rewards
  UNION ALL
  SELECT * FROM rewards_for_all
  UNION ALL
  SELECT * FROM rewards_for_all_earners_stakers
  UNION ALL
  SELECT * FROM rewards_for_all_earners_operators
{{ if .enableRewardsV2 }}
  UNION ALL
  SELECT * FROM operator_od_rewards
  UNION ALL
  SELECT * FROM staker_od_rewards
  UNION ALL
  SELECT * FROM avs_od_rewards
{{ end }}
{{ if .enableRewardsV2_1 }}
  UNION ALL
  SELECT * FROM operator_od_operator_set_rewards
  UNION ALL
  SELECT * FROM staker_od_operator_set_rewards
  UNION ALL
  SELECT * FROM avs_od_operator_set_rewards
{{ end }}
),
-- Dedupe earners, primarily operators who are also their own staker.
deduped_earners AS (
  SELECT
    earner,
    snapshot,
    reward_hash,
    token,
    SUM(amount) as amount
  FROM combined_rewards
  GROUP BY
    earner,
    snapshot,
    reward_hash,
    token
)
SELECT *, {{.generatedRewardsSnapshotId}} as generated_rewards_snapshot_id
FROM deduped_earners
ON CONFLICT (earner, token, reward_hash, snapshot) DO NOTHING
`

func (rc *RewardsCalculator) GenerateGoldFinalTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	destTableName := rewardsUtils.RewardsTable_GoldTable

	rc.logger.Sugar().Infow("Generating gold final",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	isRewardsV2Enabled, err := rc.globalConfig.IsRewardsV2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}
	rc.logger.Sugar().Infow("Is RewardsV2 enabled?", "enabled", isRewardsV2Enabled)

	isRewardsV2_1Enabled, err := rc.globalConfig.IsRewardsV2_1EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}
	rc.logger.Sugar().Infow("Is RewardsV2_1 enabled?", "enabled", isRewardsV2_1Enabled)

	query, err := rewardsUtils.RenderQueryTemplate(_15_goldFinalQuery, map[string]interface{}{
		"destTableName":                           destTableName,
		"stakerRewardAmountsTable":                rewardsUtils.RewardsTable_2_StakerRewardAmounts,
		"operatorRewardAmountsTable":              rewardsUtils.RewardsTable_3_OperatorRewardAmounts,
		"rewardsForAllTable":                      rewardsUtils.RewardsTable_4_RewardsForAll,
		"rfaeStakerTable":                         rewardsUtils.RewardsTable_5_RfaeStakers,
		"rfaeOperatorTable":                       rewardsUtils.RewardsTable_6_RfaeOperators,
		"operatorODRewardAmountsTable":            rewardsUtils.RewardsTable_8_OperatorODRewardAmounts,
		"stakerODRewardAmountsTable":              rewardsUtils.RewardsTable_9_StakerODRewardAmounts,
		"avsODRewardAmountsTable":                 rewardsUtils.RewardsTable_10_AvsODRewardAmounts,
		"enableRewardsV2":                         isRewardsV2Enabled,
		"operatorODOperatorSetRewardAmountsTable": rewardsUtils.RewardsTable_12_OperatorODOperatorSetRewardAmounts,
		"stakerODOperatorSetRewardAmountsTable":   rewardsUtils.RewardsTable_13_StakerODOperatorSetRewardAmounts,
		"avsODOperatorSetRewardAmountsTable":      rewardsUtils.RewardsTable_14_AvsODOperatorSetRewardAmounts,
		"enableRewardsV2_1":                       isRewardsV2_1Enabled,
		"generatedRewardsSnapshotId":              generatedRewardsSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_final", "error", res.Error)
		return res.Error
	}
	return nil
}

type GoldRow struct {
	Earner     string
	Snapshot   time.Time
	RewardHash string
	Token      string
	Amount     string
}

func (rc *RewardsCalculator) ListGoldRows() ([]*GoldRow, error) {
	var goldRows []*GoldRow
	res := rc.grm.Raw("select * from gold_table").Scan(&goldRows)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list gold rows", "error", res.Error)
		return nil, res.Error
	}
	return goldRows, nil
}
