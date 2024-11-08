package rewards

import (
	"go.uber.org/zap"
)

const _6_goldRfaeOperatorsQuery = `
create table {{.destTableName}} as
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
SELECT * FROM distinct_operators
`

func (rc *RewardsCalculator) GenerateGold6RfaeOperatorsTable(snapshotDate string) error {
	allTableNames := getGoldTableNames(snapshotDate)
	destTableName := allTableNames[Table_6_RfaeOperators]

	rc.logger.Sugar().Infow("Generating rfae operators table",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := renderQueryTemplate(_6_goldRfaeOperatorsQuery, map[string]string{
		"destTableName":    destTableName,
		"rfaeStakersTable": allTableNames[Table_5_RfaeStakers],
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
