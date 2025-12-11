package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _20_goldAvsOperatorSetTotalStakeRewardsQuery = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Get operators not registered for the operator set
WITH not_registered_operators AS (
    SELECT
        ap.reward_hash,
        ap.snapshot AS snapshot,
        ap.token,
        ap.tokens_per_registered_snapshot_decimal,
        ap.avs AS avs,
        ap.operator_set_id AS operator_set_id,
        ap.operator AS operator,
        ap.strategy,
        ap.multiplier
    FROM {{.activeODRewardsTable}} ap
    WHERE
        ap.num_registered_snapshots = 0
),

-- Step 2: Dedupe and sum refunds for non-registered operators
distinct_not_registered_operators AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY reward_hash, snapshot, operator
                ORDER BY strategy ASC
            ) AS rn
        FROM not_registered_operators
    ) t
    WHERE rn = 1
),

-- Step 3: Calculate refund amounts for non-registered operators
avs_operator_refund_sums AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        avs,
        operator_set_id,
        operator,
        SUM(tokens_per_registered_snapshot_decimal) OVER (PARTITION BY reward_hash, snapshot) AS avs_tokens
    FROM distinct_not_registered_operators
),

-- Step 4: Get registered operators
registered_operators AS (
    SELECT
        ap.reward_hash,
        ap.snapshot,
        ap.token,
        ap.tokens_per_registered_snapshot_decimal,
        ap.avs,
        ap.operator_set_id,
        ap.operator,
        ap.strategy,
        ap.multiplier
    FROM {{.activeODRewardsTable}} ap
    JOIN operator_set_operator_registration_snapshots osor
        ON ap.avs = osor.avs
        AND ap.operator_set_id = osor.operator_set_id
        AND ap.snapshot = osor.snapshot
        AND ap.operator = osor.operator
    WHERE ap.num_registered_snapshots != 0
),

-- Step 5: Find operators without sufficient total stake
operators_without_total_stake AS (
    SELECT
        ro.*
    FROM registered_operators ro
    LEFT JOIN {{.operatorShareSnapshotsTable}} oss
        ON ro.operator = oss.operator
        AND ro.strategy = oss.strategy
        AND ro.snapshot = oss.snapshot
    WHERE oss.operator IS NULL OR oss.shares = 0
),

-- Step 6: Dedupe and calculate refunds for operators without total stake
distinct_operators_without_stake AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY reward_hash, snapshot, operator
                ORDER BY strategy ASC
            ) AS rn
        FROM operators_without_total_stake
    ) t
    WHERE rn = 1
),

-- Step 7: Calculate total refund for operators without stake
avs_no_stake_refund_sums AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        avs,
        operator_set_id,
        operator,
        SUM(tokens_per_registered_snapshot_decimal) OVER (PARTITION BY reward_hash, snapshot) AS avs_tokens
    FROM distinct_operators_without_stake
),

-- Step 8: Find registered operators WITH stake
registered_operators_with_stake AS (
    SELECT
        ro.*
    FROM registered_operators ro
    JOIN {{.operatorShareSnapshotsTable}} oss
        ON ro.operator = oss.operator
        AND ro.strategy = oss.strategy
        AND ro.snapshot = oss.snapshot
    WHERE oss.shares > 0
),

-- Step 9: Check if strategies are registered for the operator set
strategies_registered AS (
    SELECT DISTINCT
        ro.reward_hash,
        ro.snapshot,
        ro.avs,
        ro.operator_set_id
    FROM registered_operators_with_stake ro
    JOIN operator_set_strategy_registration_snapshots ossr
        ON ro.avs = ossr.avs
        AND ro.operator_set_id = ossr.operator_set_id
        AND ro.snapshot = ossr.snapshot
        AND ro.strategy = ossr.strategy
),

-- Step 10: Find operators with stake but no strategies registered
strategies_not_registered AS (
    SELECT
        ro.*
    FROM registered_operators_with_stake ro
    LEFT JOIN strategies_registered sr
        ON ro.reward_hash = sr.reward_hash
        AND ro.snapshot = sr.snapshot
        AND ro.avs = sr.avs
        AND ro.operator_set_id = sr.operator_set_id
    WHERE sr.reward_hash IS NULL
),

-- Step 11: Calculate staker splits for refunds
staker_splits AS (
    SELECT
        snr.*,
        snr.tokens_per_registered_snapshot_decimal - FLOOR(snr.tokens_per_registered_snapshot_decimal * COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS NUMERIC)) AS staker_split
    FROM strategies_not_registered snr
    LEFT JOIN operator_set_split_snapshots oss
        ON snr.operator = oss.operator
        AND snr.avs = oss.avs
        AND snr.operator_set_id = oss.operator_set_id
        AND snr.snapshot = oss.snapshot
    LEFT JOIN default_operator_split_snapshots dos ON (snr.snapshot = dos.snapshot)
),

-- Step 12: Dedupe staker splits
distinct_staker_splits AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY reward_hash, snapshot, operator
                ORDER BY strategy ASC
            ) AS rn
        FROM staker_splits
    ) t
    WHERE rn = 1
),

-- Step 13: Sum staker refunds
avs_staker_refund_sums AS (
    SELECT
        reward_hash,
        snapshot,
        token,
        avs,
        operator_set_id,
        operator,
        SUM(staker_split) OVER (PARTITION BY reward_hash, snapshot) AS avs_tokens
    FROM distinct_staker_splits
),

-- Step 14: Combine all refund cases
combined_avs_refund_amounts AS (
    SELECT * FROM avs_operator_refund_sums
    UNION ALL
    SELECT * FROM avs_no_stake_refund_sums
    UNION ALL
    SELECT * FROM avs_staker_refund_sums
)

-- Output the final table
SELECT * FROM combined_avs_refund_amounts
`

func (rc *RewardsCalculator) GenerateGold20AvsOperatorSetTotalStakeRewardsTable(snapshotDate string, forks config.ForkMap) error {
	rewardsV2_2Enabled, err := rc.globalConfig.IsRewardsV2_2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.2 is enabled", "error", err)
		return err
	}
	if !rewardsV2_2Enabled {
		rc.logger.Sugar().Infow("Rewards v2.2 is not enabled, skipping v2.2 table 20")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_20_AvsOperatorSetTotalStakeRewards]

	rc.logger.Sugar().Infow("Generating v2.2 AVS operator set reward refunds with total stake validation",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
		zap.String("coloradoHardforkDate", forks[config.RewardsFork_Colorado].Date),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_20_goldAvsOperatorSetTotalStakeRewardsQuery, map[string]interface{}{
		"destTableName":               destTableName,
		"activeODRewardsTable":        allTableNames[rewardsUtils.Table_11_ActiveODOperatorSetRewards],
		"operatorShareSnapshotsTable": "operator_share_snapshots",
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query, sql.Named("coloradoHardforkDate", forks[config.RewardsFork_Colorado].Date))
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_avs_operator_set_total_stake_rewards v2.2", "error", res.Error)
		return res.Error
	}
	return nil
}
