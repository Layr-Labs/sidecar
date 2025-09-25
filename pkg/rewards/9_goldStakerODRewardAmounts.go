package rewards

import (
	"database/sql"
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _9_goldStakerODRewardAmountsQuery = `
create table {{.destTableName}} as

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
    LEFT JOIN default_operator_split_snapshots dos ON (rso.snapshot = dos.snapshot)
),
-- Get the latest available staker delegation snapshot on or before the reward snapshot date
latest_staker_delegation_snapshots AS (
    SELECT 
        ors.reward_hash,
        ors.snapshot as reward_snapshot,
        ors.operator,
        (SELECT MAX(sds.snapshot) 
         FROM staker_delegation_snapshots sds 
         WHERE sds.operator = ors.operator 
           AND sds.snapshot <= ors.snapshot) as latest_delegation_snapshot
    FROM staker_splits ors
),
-- Get the stakers that were delegated to the operator for the snapshot
staker_delegated_operators AS (
    SELECT
        ors.*,
        sds.staker
    FROM staker_splits ors
    JOIN latest_staker_delegation_snapshots lds ON 
        ors.reward_hash = lds.reward_hash AND
        ors.snapshot = lds.reward_snapshot AND
        ors.operator = lds.operator
    JOIN staker_delegation_snapshots sds ON
        sds.operator = lds.operator AND
        sds.snapshot = lds.latest_delegation_snapshot
),

-- Get the shares for stakers delegated to the operator
-- Use conservative matching for share snapshots (same approach as Table 5)
staker_avs_strategy_shares AS (
    SELECT
        sdo.*,
        sss.shares
    FROM staker_delegated_operators sdo
    JOIN staker_share_snapshots sss ON
        sdo.staker = sss.staker AND
        sdo.strategy = sss.strategy AND
        sss.snapshot = (
            SELECT MAX(sss2.snapshot) 
            FROM staker_share_snapshots sss2 
            WHERE sss2.staker = sdo.staker 
              AND sss2.strategy = sdo.strategy 
              AND sss2.snapshot <= sdo.snapshot
        )
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
SELECT *, {{.generatedRewardsSnapshotId}} as generated_rewards_snapshot_id FROM staker_reward_amounts
`

func (rc *RewardsCalculator) GenerateGold9StakerODRewardAmountsTable(snapshotDate string, generatedRewardsSnapshotId uint64, forks config.ForkMap) error {
	rewardsV2Enabled, err := rc.globalConfig.IsRewardsV2EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2 is enabled", "error", err)
		return err
	}
	if !rewardsV2Enabled {
		rc.logger.Sugar().Infow("Rewards v2 is not enabled for this cutoff date, skipping GenerateGold9StakerODRewardAmountsTable")
		return nil
	}

	destTableName := rc.getTempStakerODRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId)
	activeOdRewardsTableName := rc.getTempActiveODRewardsTableName(snapshotDate, generatedRewardsSnapshotId)

	// Drop existing temp table
	if err := rc.DropTempStakerODRewardAmountsTable(snapshotDate, generatedRewardsSnapshotId); err != nil {
		rc.logger.Sugar().Errorw("Failed to drop existing temp staker OD reward amounts table", "error", err)
		return err
	}

	rc.logger.Sugar().Infow("Generating temp Staker OD reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
		zap.String("trinityHardforkDate", forks[config.RewardsFork_Trinity].Date),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_9_goldStakerODRewardAmountsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"activeODRewardsTable":       activeOdRewardsTableName,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query, sql.Named("trinityHardforkDate", forks[config.RewardsFork_Trinity].Date))
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create temp staker OD reward amounts", "error", res.Error)
		return res.Error
	}
	return nil
}

// Helper functions for temp table management
func (rc *RewardsCalculator) getTempStakerODRewardAmountsTableName(snapshotDate string, generatedRewardSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(snapshotDate)
	return fmt.Sprintf("tmp_rewards_gold_9_staker_od_reward_amounts_%s_%d", camelDate, generatedRewardSnapshotId)
}

func (rc *RewardsCalculator) DropTempStakerODRewardAmountsTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := rc.getTempStakerODRewardAmountsTableName(snapshotDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to drop temp staker OD reward amounts table", "error", res.Error)
		return res.Error
	}
	rc.logger.Sugar().Infow("Successfully dropped temp staker OD reward amounts table",
		zap.String("tempTableName", tempTableName),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	return nil
}
