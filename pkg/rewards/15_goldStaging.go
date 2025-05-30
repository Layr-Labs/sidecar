package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _15_goldStagingQuery = `
INSERT INTO {{.destTableName}} (earner, snapshot, reward_hash, token, amount, generated_rewards_snapshot_id) as
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
SELECT *, @generatedRewardsSnapshotId as generated_rewards_snapshot_id
FROM deduped_earners
`

func (rc *RewardsCalculator) GenerateGold15StagingTable(snapshotDate string, generatedSnapshotId uint64) error {
	destTableName := rewardsUtils.RewardsTable_GoldStaging

	rc.logger.Sugar().Infow("Generating gold staging",
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

	query, err := rewardsUtils.RenderQueryTemplate(_15_goldStagingQuery, map[string]interface{}{
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
		"generatedRewardsSnapshotId":              generatedSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	query = query + " ON CONFLICT (earner, token, reward_hash, snapshot) DO NOTHING"

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_staging", "error", res.Error)
		return res.Error
	}
	return nil
}

type GoldStagingRow struct {
	Earner     string
	Snapshot   string
	RewardHash string
	Token      string
	Amount     string
}

func (rc *RewardsCalculator) ListGoldStagingRowsForSnapshot(snapshotDate string) ([]*GoldStagingRow, error) {
	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)

	results := make([]*GoldStagingRow, 0)
	query := `
	SELECT
		earner,
		snapshot::text as snapshot,
		reward_hash,
		token,
		amount
	FROM {{.goldStagingTable}} WHERE DATE(snapshot) < @cutoffDate`
	query, err := rewardsUtils.RenderQueryTemplate(query, map[string]interface{}{
		"goldStagingTable": allTableNames[rewardsUtils.Table_15_GoldStaging],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return nil, err
	}
	res := rc.grm.Raw(query,
		sql.Named("cutoffDate", snapshotDate),
	).Scan(&results)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list gold staging rows", "error", res.Error)
		return nil, res.Error
	}
	return results, nil
}
