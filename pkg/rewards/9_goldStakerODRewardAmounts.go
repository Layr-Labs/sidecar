package rewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _9_goldStakerODRewardAmountsQuery = `
CREATE TABLE {{.destTableName}} AS

-- Step 1: Get the rows where operators have registered for the AVS
WITH reward_snapshot_operators AS (
    SELECT
        ap.reward_hash,
        ap.snapshot AS snapshot,
        ap.token,
        ap.tokens_per_registered_snapshot_decimal,
        ap.avs AS avs,
        ap.operator AS operator,
        ap.strategy,
        ap.multiplier,
        ap.reward_submission_date
    FROM {{.activeODRewardsTable}} ap
    JOIN operator_avs_registration_snapshots oar
        ON ap.avs = oar.avs 
       AND ap.snapshot = oar.snapshot 
       AND ap.operator = oar.operator
),

-- Calculate the total staker split for each operator reward with dynamic split logic
-- If no split is found, default to 1000 (10%)
staker_splits AS (
    SELECT 
        rso.*,
        CASE
            WHEN rso.snapshot < @trinityHardforkDate AND rso.reward_submission_date < @trinityHardforkDate THEN    
                rso.tokens_per_registered_snapshot_decimal - FLOOR(rso.tokens_per_registered_snapshot_decimal * COALESCE(oas.split, 1000) / CAST(10000 AS DECIMAL))
            ELSE
                rso.tokens_per_registered_snapshot_decimal - FLOOR(rso.tokens_per_registered_snapshot_decimal * COALESCE(oas.split, dos.split, 1000) / CAST(10000 AS DECIMAL))
        END AS staker_split
    FROM reward_snapshot_operators rso
    LEFT JOIN operator_avs_split_snapshots oas
        ON rso.operator = oas.operator 
       AND rso.avs = oas.avs 
       AND rso.snapshot = oas.snapshot
    LEFT JOIN default_operator_split_snapshots dos
        ON rso.snapshot = dos.snapshot
),
-- Get the stakers that were delegated to the operator for the snapshot
staker_delegated_operators AS (
    SELECT
        ors.*,
        sds.staker
    FROM staker_splits ors
    JOIN staker_delegation_snapshots sds
        ON ors.operator = sds.operator 
       AND ors.snapshot = sds.snapshot
),

-- Get the shares for stakers delegated to the operator
staker_avs_strategy_shares AS (
    SELECT
        sdo.*,
        sss.shares
    FROM staker_delegated_operators sdo
    JOIN staker_share_snapshots sss
        ON sdo.staker = sss.staker 
       AND sdo.snapshot = sss.snapshot 
       AND sdo.strategy = sss.strategy
    -- Filter out negative shares and zero multiplier to avoid division by zero
    WHERE sss.shares > 0 AND sdo.multiplier != 0
),

-- Calculate the weight of each staker
staker_weights AS (
    SELECT 
        *,
        SUM(multiplier * shares) OVER (PARTITION BY staker, reward_hash, snapshot) AS staker_weight
    FROM staker_avs_strategy_shares
),
-- Get distinct stakers since their weights are already calculated
distinct_stakers AS (
    SELECT *
    FROM (
        SELECT 
            *,
            -- We can use an arbitrary order here since the staker_weight is the same for each (staker, strategy, hash, snapshot)
            -- We use strategy ASC for better debuggability
            ROW_NUMBER() OVER (
                PARTITION BY reward_hash, snapshot, staker 
                ORDER BY strategy ASC
            ) AS rn
        FROM staker_weights
    ) t
    WHERE rn = 1
    ORDER BY reward_hash, snapshot, staker
),
-- Calculate the sum of all staker weights for each reward and snapshot
staker_weight_sum AS (
    SELECT 
        *,
        SUM(staker_weight) OVER (PARTITION BY reward_hash, operator, snapshot) AS total_weight
    FROM distinct_stakers
),
-- Calculate staker proportion of tokens for each reward and snapshot
staker_proportion AS (
    SELECT 
        *,
        FLOOR((staker_weight / total_weight) * 1000000000000000) / 1000000000000000 AS staker_proportion
    FROM staker_weight_sum
),
-- Calculate the staker reward amounts
staker_reward_amounts AS (
    SELECT 
        *,
        FLOOR(staker_proportion * staker_split) AS staker_tokens
    FROM staker_proportion
)
-- Output the final table
SELECT * FROM staker_reward_amounts
`

func (rc *RewardsCalculator) GenerateGold9StakerODRewardAmountsTable(snapshotDate string, forks config.ForkMap) error {
	rewardsV2Enabled, err := rc.globalConfig.IsRewardsV2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}
	if !rewardsV2Enabled {
		rc.logger.Sugar().Infow("Rewards v2 is not enabled for this cutoff date, skipping GenerateGold9StakerODRewardAmountsTable")
		return nil
	}

	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)
	destTableName := allTableNames[rewardsUtils.Table_9_StakerODRewardAmounts]

	rc.logger.Sugar().Infow("Generating Staker OD reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
		zap.String("trinityHardforkDate", forks[config.Fork_Trinity]),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_9_goldStakerODRewardAmountsQuery, map[string]interface{}{
		"destTableName":        destTableName,
		"activeODRewardsTable": allTableNames[rewardsUtils.Table_7_ActiveODRewards],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query,
		sql.Named("trinityHardforkDate", forks[config.Fork_Trinity]),
	)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_staker_od_reward_amounts", "error", res.Error)
		return res.Error
	}
	return nil
}
