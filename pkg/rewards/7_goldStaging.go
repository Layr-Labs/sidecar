package rewards

import (
	"database/sql"
	"go.uber.org/zap"
)

const _7_goldStagingQuery = `
create table {{.destTableName}} as
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
    staker_tokens as amounts
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
SELECT *
FROM deduped_earners
`

func (rc *RewardsCalculator) GenerateGold7StagingTable(snapshotDate string) error {
	allTableNames := getGoldTableNames(snapshotDate)
	destTableName := allTableNames[Table_7_GoldStaging]

	rc.logger.Sugar().Infow("Generating gold staging",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := renderQueryTemplate(_7_goldStagingQuery, map[string]string{
		"destTableName":              destTableName,
		"stakerRewardAmountsTable":   allTableNames[Table_2_StakerRewardAmounts],
		"operatorRewardAmountsTable": allTableNames[Table_3_OperatorRewardAmounts],
		"rewardsForAllTable":         allTableNames[Table_4_RewardsForAll],
		"rfaeStakerTable":            allTableNames[Table_5_RfaeStakers],
		"rfaeOperatorTable":          allTableNames[Table_6_RfaeOperators],
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

type GoldStagingRow struct {
	Earner     string
	Snapshot   string
	RewardHash string
	Token      string
	Amount     string
}

func (rc *RewardsCalculator) ListGoldStagingRowsForSnapshot(snapshotDate string) ([]*GoldStagingRow, error) {
	allTableNames := getGoldTableNames(snapshotDate)

	results := make([]*GoldStagingRow, 0)
	query, err := renderQueryTemplate(`
	SELECT
		earner,
		snapshot::text as snapshot,
		reward_hash,
		token,
		amount
	FROM {{.goldStagingTable}} WHERE DATE(snapshot) < @cutoffDate`, map[string]string{
		"goldStagingTable": allTableNames[Table_7_GoldStaging],
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
