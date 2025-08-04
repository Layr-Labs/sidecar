package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _15_goldStagingQuery = `
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

// Optimized query that only processes new data since the last snapshot
// const _15_goldStagingIncrementalQuery = `
// INSERT INTO {{.destTableName}} (earner, snapshot, reward_hash, token, amount, generated_rewards_snapshot_id)
// WITH staker_rewards AS (
//   -- We can select DISTINCT here because the staker's tokens are the same for each strategy in the reward hash
//   SELECT DISTINCT
//     staker as earner,
//     snapshot,
//     reward_hash,
//     token,
//     staker_tokens as amount
//   FROM {{.stakerRewardAmountsTable}}
//   WHERE snapshot > {{.lastSnapshotDate}}::date
// ),
// operator_rewards AS (
//   SELECT DISTINCT
//     -- We can select DISTINCT here because the operator's tokens are the same for each strategy in the reward hash
//     operator as earner,
//     snapshot,
//     reward_hash,
//     token,
//     operator_tokens as amount
//   FROM {{.operatorRewardAmountsTable}}
//   WHERE snapshot > {{.lastSnapshotDate}}::date
// ),
// rewards_for_all AS (
//   SELECT DISTINCT
//     staker as earner,
//     snapshot,
//     reward_hash,
//     token,
//     staker_tokens as amount
//   FROM {{.rewardsForAllTable}}
//   WHERE snapshot > {{.lastSnapshotDate}}::date
// ),
// rewards_for_all_earners_stakers AS (
//   SELECT DISTINCT
//     staker as earner,
//     snapshot,
//     reward_hash,
//     token,
//     staker_tokens as amount
//   FROM {{.rfaeStakerTable}}
//   WHERE snapshot > {{.lastSnapshotDate}}::date
// ),
// rewards_for_all_earners_operators AS (
//   SELECT DISTINCT
//     operator as earner,
//     snapshot,
//     reward_hash,
//     token,
//     operator_tokens as amount
//   FROM {{.rfaeOperatorTable}}
//   WHERE snapshot > {{.lastSnapshotDate}}::date
// ),
// {{ if .enableRewardsV2 }}
// operator_od_rewards AS (
//   SELECT DISTINCT
//     -- We can select DISTINCT here because the operator's tokens are the same for each strategy in the reward hash
//     operator as earner,
//     snapshot,
//     reward_hash,
//     token,
//     operator_tokens as amount
//   FROM {{.operatorODRewardAmountsTable}}
//   WHERE snapshot > {{.lastSnapshotDate}}::date
// ),
// staker_od_rewards AS (
//   SELECT DISTINCT
//     -- We can select DISTINCT here because the staker's tokens are the same for each strategy in the reward hash
//     staker as earner,
//     snapshot,
//     reward_hash,
//     token,
//     staker_tokens as amount
//   FROM {{.stakerODRewardAmountsTable}}
//   WHERE snapshot > {{.lastSnapshotDate}}::date
// ),
// avs_od_rewards AS (
//   SELECT DISTINCT
//     -- We can select DISTINCT here because the avs's tokens are the same for each strategy in the reward hash
//     avs as earner,
//     snapshot,
//     reward_hash,
//     token,
//     avs_tokens as amount
//   FROM {{.avsODRewardAmountsTable}}
//   WHERE snapshot > {{.lastSnapshotDate}}::date
// ),
// {{ end }}
// {{ if .enableRewardsV2_1 }}
// operator_od_operator_set_rewards AS (
//   SELECT DISTINCT
//     -- We can select DISTINCT here because the operator's tokens are the same for each strategy in the reward hash
//     operator as earner,
//     snapshot,
//     reward_hash,
//     token,
//     operator_tokens as amount
//   FROM {{.operatorODOperatorSetRewardAmountsTable}}
//   WHERE snapshot > {{.lastSnapshotDate}}::date
// ),
// staker_od_operator_set_rewards AS (
//   SELECT DISTINCT
//     -- We can select DISTINCT here because the staker's tokens are the same for each strategy in the reward hash
//     staker as earner,
//     snapshot,
//     reward_hash,
//     token,
//     staker_tokens as amount
//   FROM {{.stakerODOperatorSetRewardAmountsTable}}
//   WHERE snapshot > {{.lastSnapshotDate}}::date
// ),
// avs_od_operator_set_rewards AS (
//   SELECT DISTINCT
//     -- We can select DISTINCT here because the avs's tokens are the same for each strategy in the reward hash
//     avs as earner,
//     snapshot,
//     reward_hash,
//     token,
//     avs_tokens as amount
//   FROM {{.avsODOperatorSetRewardAmountsTable}}
//   WHERE snapshot > {{.lastSnapshotDate}}::date
// ),
// {{ end }}
// combined_rewards AS (
//   SELECT * FROM operator_rewards
//   UNION ALL
//   SELECT * FROM staker_rewards
//   UNION ALL
//   SELECT * FROM rewards_for_all
//   UNION ALL
//   SELECT * FROM rewards_for_all_earners_stakers
//   UNION ALL
//   SELECT * FROM rewards_for_all_earners_operators
// {{ if .enableRewardsV2 }}
//   UNION ALL
//   SELECT * FROM operator_od_rewards
//   UNION ALL
//   SELECT * FROM staker_od_rewards
//   UNION ALL
//   SELECT * FROM avs_od_rewards
// {{ end }}
// {{ if .enableRewardsV2_1 }}
//   UNION ALL
//   SELECT * FROM operator_od_operator_set_rewards
//   UNION ALL
//   SELECT * FROM staker_od_operator_set_rewards
//   UNION ALL
//   SELECT * FROM avs_od_operator_set_rewards
// {{ end }}
// ),
// -- Dedupe earners, primarily operators who are also their own staker.
// deduped_earners AS (
//   SELECT
//     earner,
//     snapshot,
//     reward_hash,
//     token,
//     SUM(amount) as amount
//   FROM combined_rewards
//   GROUP BY
//     earner,
//     snapshot,
//     reward_hash,
//     token
// )
// SELECT *, {{.generatedRewardsSnapshotId}} as generated_rewards_snapshot_id
// FROM deduped_earners
// ON CONFLICT (earner, token, reward_hash, snapshot) DO NOTHING
// `

func (rc *RewardsCalculator) GenerateGold15StagingTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
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
		"generatedRewardsSnapshotId":              generatedRewardsSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_staging", "error", res.Error)
		return res.Error
	}
	return nil
}

// GenerateGold15StagingTableIncremental generates the gold staging table incrementally,
// only processing new data since the last completed snapshot
// func (rc *RewardsCalculator) GenerateGold15StagingTableIncremental(snapshotDate string, generatedRewardsSnapshotId uint64) error {
// 	destTableName := rewardsUtils.RewardsTable_GoldStaging

// 	rc.logger.Sugar().Infow("Generating gold staging incrementally",
// 		zap.String("cutoffDate", snapshotDate),
// 		zap.String("destTableName", destTableName),
// 	)

// 	// Find the last completed snapshot date
// 	lastSnapshotDate, err := rc.getLastCompletedSnapshotDate(snapshotDate)
// 	if err != nil {
// 		rc.logger.Sugar().Errorw("Failed to get last completed snapshot date", "error", err)
// 		return err
// 	}

// 	// If no previous snapshot exists, fall back to full generation
// 	if lastSnapshotDate == "" {
// 		rc.logger.Sugar().Infow("No previous snapshot found, falling back to full generation")
// 		return rc.GenerateGold15StagingTable(snapshotDate, generatedRewardsSnapshotId)
// 	}

// 	rc.logger.Sugar().Infow("Using incremental generation",
// 		zap.String("lastSnapshotDate", lastSnapshotDate),
// 		zap.String("currentSnapshotDate", snapshotDate),
// 	)

// 	isRewardsV2Enabled, err := rc.globalConfig.IsRewardsV2EnabledForCutoffDate(snapshotDate)
// 	if err != nil {
// 		rc.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
// 		return err
// 	}

// 	isRewardsV2_1Enabled, err := rc.globalConfig.IsRewardsV2_1EnabledForCutoffDate(snapshotDate)
// 	if err != nil {
// 		rc.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
// 		return err
// 	}

// 	query, err := rewardsUtils.RenderQueryTemplate(_15_goldStagingIncrementalQuery, map[string]interface{}{
// 		"destTableName":                           destTableName,
// 		"stakerRewardAmountsTable":                rewardsUtils.RewardsTable_2_StakerRewardAmounts,
// 		"operatorRewardAmountsTable":              rewardsUtils.RewardsTable_3_OperatorRewardAmounts,
// 		"rewardsForAllTable":                      rewardsUtils.RewardsTable_4_RewardsForAll,
// 		"rfaeStakerTable":                         rewardsUtils.RewardsTable_5_RfaeStakers,
// 		"rfaeOperatorTable":                       rewardsUtils.RewardsTable_6_RfaeOperators,
// 		"operatorODRewardAmountsTable":            rewardsUtils.RewardsTable_8_OperatorODRewardAmounts,
// 		"stakerODRewardAmountsTable":              rewardsUtils.RewardsTable_9_StakerODRewardAmounts,
// 		"avsODRewardAmountsTable":                 rewardsUtils.RewardsTable_10_AvsODRewardAmounts,
// 		"enableRewardsV2":                         isRewardsV2Enabled,
// 		"operatorODOperatorSetRewardAmountsTable": rewardsUtils.RewardsTable_12_OperatorODOperatorSetRewardAmounts,
// 		"stakerODOperatorSetRewardAmountsTable":   rewardsUtils.RewardsTable_13_StakerODOperatorSetRewardAmounts,
// 		"avsODOperatorSetRewardAmountsTable":      rewardsUtils.RewardsTable_14_AvsODOperatorSetRewardAmounts,
// 		"enableRewardsV2_1":                       isRewardsV2_1Enabled,
// 		"generatedRewardsSnapshotId":              generatedRewardsSnapshotId,
// 		"lastSnapshotDate":                        fmt.Sprintf("'%s'", lastSnapshotDate),
// 	})
// 	if err != nil {
// 		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
// 		return err
// 	}

// 	res := rc.grm.Exec(query)
// 	if res.Error != nil {
// 		rc.logger.Sugar().Errorw("Failed to create gold_staging incrementally", "error", res.Error)
// 		return res.Error
// 	}

// 	rc.logger.Sugar().Infow("Gold staging incremental generation completed",
// 		zap.Int64("rowsAffected", res.RowsAffected),
// 	)
// 	return nil
// }

// // getLastCompletedSnapshotDate returns the snapshot date of the last completed rewards calculation
// func (rc *RewardsCalculator) getLastCompletedSnapshotDate(currentSnapshotDate string) (string, error) {
// 	query := `
// 		SELECT snapshot_date
// 		FROM generated_rewards_snapshots
// 		WHERE status = 'complete'
// 		  AND snapshot_date < @currentSnapshotDate
// 		ORDER BY snapshot_date DESC
// 		LIMIT 1
// 	`

// 	var lastSnapshotDate sql.NullString
// 	res := rc.grm.Raw(query, sql.Named("currentSnapshotDate", currentSnapshotDate)).Scan(&lastSnapshotDate)
// 	if res.Error != nil {
// 		return "", res.Error
// 	}

// 	if !lastSnapshotDate.Valid {
// 		return "", nil
// 	}

// 	return lastSnapshotDate.String, nil
// }

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
