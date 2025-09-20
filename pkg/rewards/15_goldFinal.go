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
),
rewards_for_all AS (
  SELECT DISTINCT
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amount
  FROM {{.rewardsForAllTable}}
),
rewards_for_all_earners_stakers AS (
  SELECT DISTINCT
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amount
  FROM {{.rfaeStakerTable}}
),
rewards_for_all_earners_operators AS (
  SELECT DISTINCT
    operator as earner,
    snapshot,
    reward_hash,
    token,
    operator_tokens as amount
  FROM {{.rfaeOperatorTable}}
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
		"stakerRewardAmountsTable":                rc.getTempStakerRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId),
		"operatorRewardAmountsTable":              rc.getTempOperatorRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId),
		"rewardsForAllTable":                      rc.getTempRewardsForAllTableName(snapshotDate, generatedRewardsSnapshotId),
		"rfaeStakerTable":                         rc.getTempRfaeStakersTableName(snapshotDate, generatedRewardsSnapshotId),
		"rfaeOperatorTable":                       rc.getTempRfaeOperatorsTableName(snapshotDate, generatedRewardsSnapshotId),
		"operatorODRewardAmountsTable":            rc.getTempOperatorODRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId),
		"stakerODRewardAmountsTable":              rc.getTempStakerODRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId),
		"avsODRewardAmountsTable":                 rc.getTempAvsODRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId),
		"enableRewardsV2":                         isRewardsV2Enabled,
		"operatorODOperatorSetRewardAmountsTable": rc.getTempOperatorODOperatorSetRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId),
		"stakerODOperatorSetRewardAmountsTable":   rc.getTempStakerODOperatorSetRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId),
		"avsODOperatorSetRewardAmountsTable":      rc.getTempAvsODOperatorSetRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId),
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
