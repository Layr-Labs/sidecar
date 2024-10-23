package rewards

import "database/sql"

const _7_goldStagingQuery = `
insert into gold_7_staging
WITH staker_rewards AS (
  -- We can select DISTINCT here because the staker's tokens are the same for each strategy in the reward hash
  SELECT DISTINCT
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amount
  FROM gold_2_staker_reward_amounts
),
operator_rewards AS (
  SELECT DISTINCT
    -- We can select DISTINCT here because the operator's tokens are the same for each strategy in the reward hash
    operator as earner,
    snapshot,
    reward_hash,
    token,
    operator_tokens as amount
  FROM gold_3_operator_reward_amounts
),
rewards_for_all AS (
  SELECT DISTINCT
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amount
  FROM gold_4_rewards_for_all
),
rewards_for_all_earners_stakers AS (
  SELECT DISTINCT
    staker as earner,
    snapshot,
    reward_hash,
    token,
    staker_tokens as amounts
  FROM gold_5_rfae_stakers
),
rewards_for_all_earners_operators AS (
  SELECT DISTINCT
    operator as earner,
    snapshot,
    reward_hash,
    token,
    operator_tokens as amount
  FROM gold_6_rfae_operators
),
all_combined_rewards AS (
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
    sum_big(amount) as amount
  FROM all_combined_rewards
  GROUP BY
    earner,
    snapshot,
    reward_hash,
    token
)
SELECT *
FROM deduped_earners
`

type GoldStagingRow struct {
	Earner     string
	Snapshot   string
	RewardHash string
	Token      string
	Amount     string
}

func (rc *RewardsCalculator) GenerateGold7StagingTable() error {
	res := rc.grm.Exec(_7_goldStagingQuery)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_staging", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) CreateGold7StagingTable() error {
	query := `
		create table if not exists gold_7_staging (
			earner TEXT NOT NULL,
			snapshot DATE NOT NULL,
			reward_hash TEXT NOT NULL,
			token TEXT NOT NULL,
			amount TEXT NOT NULL
		)
	`
	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_7_staging", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) ListGoldStagingRowsForSnapshot(snapshotDate string) ([]*GoldStagingRow, error) {
	results := make([]*GoldStagingRow, 0)
	query := `
	SELECT
		earner,
		cast(snapshot as text) as snapshot,
		reward_hash,
		token,
		amount
	FROM gold_7_staging WHERE DATE(snapshot) < @cutoffDate`
	res := rc.grm.Raw(query, sql.Named("cutoffDate", snapshotDate)).Scan(&results)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list gold staging rows", "error", res.Error)
		return nil, res.Error
	}
	return results, nil
}
