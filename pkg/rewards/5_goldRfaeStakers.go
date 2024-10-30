package rewards

import (
	"database/sql"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"go.uber.org/zap"
)

const _5_goldRfaeStakersQuery = `
create table {{.destTableName}} as
WITH avs_opted_operators AS (
  SELECT DISTINCT
    snapshot,
    operator
  FROM operator_avs_registration_snapshots
),
-- Get the operators who will earn rewards for the reward submission at the given snapshot
reward_snapshot_operators as (
  SELECT
    ap.reward_hash,
    ap.snapshot,
    ap.token,
    ap.tokens_per_day_decimal,
    ap.avs,
    ap.strategy,
    ap.multiplier,
    ap.reward_type,
    ap.reward_submission_date,
    aoo.operator
  FROM {{.activeRewardsTable}} ap
  JOIN avs_opted_operators aoo
  ON ap.snapshot = aoo.snapshot
  WHERE ap.reward_type = 'all_earners'
),
-- Get the stakers that were delegated to the operator for the snapshot 
staker_delegated_operators AS (
  SELECT
    rso.*,
    sds.staker
  FROM reward_snapshot_operators rso
  JOIN staker_delegation_snapshots sds
  ON
    rso.operator = sds.operator AND
    rso.snapshot = sds.snapshot
),
-- Get the shares of each strategy the staker has delegated to the operator
staker_strategy_shares AS (
  SELECT
    sdo.*,
    sss.shares
  FROM staker_delegated_operators sdo
  JOIN staker_share_snapshots sss
  ON
    sdo.staker = sss.staker AND
    sdo.snapshot = sss.snapshot AND
    sdo.strategy = sss.strategy
  -- Parse out negative shares and zero multiplier so there is no division by zero case
  WHERE sss.shares > 0 and sdo.multiplier != 0
),
addresses_to_exclude AS (
    select address as excluded_address from excluded_addresses where network = @network 
),
-- Parse out the stakers who are addresses
parsed_out_excluded_addresses AS (
  SELECT * from staker_strategy_shares sss
  LEFT JOIN addresses_to_exclude ate ON sss.staker = ate.excluded_address
    WHERE 
      -- The end result here is that null excluded addresses are not selected UNLESS after the cutoff date
      ate.excluded_address IS NULL  -- Earner is not in the exclusion list
      OR sss.snapshot >= DATE(@panamaForkDate)  -- Or snapshot is on or after the cutoff date
),
-- Calculate the weight of a staker
staker_weights AS (
  SELECT *,
    SUM(multiplier * shares) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
  FROM parsed_out_excluded_addresses
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
-- Calculate sum of all staker weights for each reward and snapshot
staker_weight_sum AS (
  SELECT *,
    SUM(staker_weight) OVER (PARTITION BY reward_hash, snapshot) as total_weight
  FROM distinct_stakers
),
-- Calculate staker proportion of tokens for each reward and snapshot
staker_proportion AS (
  SELECT *,
    FLOOR((staker_weight / total_weight) * 1000000000000000) / 1000000000000000 AS staker_proportion
  FROM staker_weight_sum
),
-- Calculate total tokens to the (staker, operator) pair
staker_operator_total_tokens AS (
  SELECT *,
    FLOOR(staker_proportion * tokens_per_day_decimal) as total_staker_operator_payout
  FROM staker_proportion
),
-- Calculate the token breakdown for each (staker, operator) pair
token_breakdowns AS (
  SELECT *,
    floor(total_staker_operator_payout * 0.10) as operator_tokens,
    total_staker_operator_payout - floor(total_staker_operator_payout * 0.10) as staker_tokens
  FROM staker_operator_total_tokens
)
SELECT * from token_breakdowns
ORDER BY reward_hash, snapshot, staker, operator
`

func (rc *RewardsCalculator) GenerateGold5RfaeStakersTable(snapshotDate string, forks config.ForkMap) error {
	allTableNames := getGoldTableNames(snapshotDate)
	destTableName := allTableNames[Table_5_RfaeStakers]

	rc.logger.Sugar().Infow("Generating rewards for all table",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := renderQueryTemplate(_5_goldRfaeStakersQuery, map[string]string{
		"destTableName":      destTableName,
		"activeRewardsTable": allTableNames[Table_1_ActiveRewards],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query,
		sql.Named("panamaForkDate", forks[config.Fork_Panama]),
		sql.Named("network", rc.globalConfig.Chain.String()),
	)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to generate gold_rfae_stakers", "error", res.Error)
		return res.Error
	}
	return nil
}
