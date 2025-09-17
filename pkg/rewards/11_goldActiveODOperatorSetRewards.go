package rewards

import (
	"database/sql"
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"

	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

var _11_goldActiveODOperatorSetRewardsQuery = `
create table {{.destTableName}} as
WITH 
-- Step 1: Modify active rewards and compute tokens per day
active_rewards_modified AS (
    SELECT 
        *,
        CAST(@cutoffDate AS TIMESTAMP(6)) AS global_end_inclusive -- Inclusive means we DO USE this day as a snapshot
    FROM operator_directed_operator_set_rewards
    WHERE end_timestamp >= TIMESTAMP '{{.rewardsStart}}'
      AND start_timestamp <= TIMESTAMP '{{.cutoffDate}}'
      AND block_time <= TIMESTAMP '{{.cutoffDate}}' -- Always ensure we're not using future data. Should never happen since we're never backfilling, but here for safety and consistency.
),

-- Step 2: Cut each reward's start and end windows to handle the global range
active_rewards_updated_end_timestamps AS (
    SELECT
        avs,
        operator_set_id,
        operator,
        /**
         * Cut the start and end windows to handle
         * A. Retroactive rewards that came recently whose start date is less than start_timestamp
         * B. Don't make any rewards past end_timestamp for this run
         */
        start_timestamp AS reward_start_exclusive,
        LEAST(global_end_inclusive, end_timestamp) AS reward_end_inclusive,
        amount,
        token,
        multiplier,
        strategy,
        reward_hash,
        duration,
        global_end_inclusive,
        block_date AS reward_submission_date
    FROM active_rewards_modified
),
-- Optimized: Get the latest snapshot for each reward hash
reward_progress AS (
    SELECT 
        reward_hash, 
        MAX(snapshot) as last_snapshot
    FROM gold_table 
    GROUP BY reward_hash
),
-- Step 3: For each reward hash, find the latest snapshot
active_rewards_updated_start_timestamps AS (
    SELECT
        ap.avs,
        ap.operator_set_id,
        ap.operator,
        COALESCE(g.last_snapshot, ap.reward_start_exclusive) AS reward_start_exclusive,
        ap.reward_end_inclusive,
        ap.token,
        -- We use floor to ensure we are always underestimating total tokens per day
        FLOOR(ap.amount) AS amount_decimal,
        ap.multiplier,
        ap.strategy,
        ap.reward_hash,
        ap.duration,
        ap.global_end_inclusive,
        ap.reward_submission_date
    FROM active_rewards_updated_end_timestamps ap
    LEFT JOIN reward_progress g 
        ON g.reward_hash = ap.reward_hash
    GROUP BY 
        ap.avs, 
        ap.operator_set_id,
        ap.operator, 
        ap.reward_end_inclusive, 
        ap.token, 
        ap.amount,
        ap.multiplier, 
        ap.strategy, 
        ap.reward_hash, 
        ap.duration, 
        ap.global_end_inclusive, 
        ap.reward_start_exclusive, 
        ap.reward_submission_date,
        g.last_snapshot
),

-- Step 4: Filter out invalid reward ranges
active_reward_ranges AS (
    /** Take out (reward_start_exclusive, reward_end_inclusive) windows where
	* 1. reward_start_exclusive >= reward_end_inclusive: The reward period is done or we will handle on a subsequent run
	*/
    SELECT * 
    FROM active_rewards_updated_start_timestamps
    WHERE reward_start_exclusive < reward_end_inclusive
),

-- Step 5: Explode out the ranges for a day per inclusive date
exploded_active_range_rewards AS (
    SELECT
        *
    FROM active_reward_ranges
    CROSS JOIN generate_series(
        DATE(reward_start_exclusive), 
        DATE(reward_end_inclusive), 
        INTERVAL '1' DAY
    ) AS day
),

-- Step 6: Prepare cleaned active rewards
active_rewards_cleaned AS (
    SELECT
        avs,
        operator_set_id,
        operator,
        CAST(day AS DATE) AS snapshot,
        token,
        amount_decimal,
        multiplier,
        strategy,
        duration,
        reward_hash,
        reward_submission_date
    FROM exploded_active_range_rewards
    -- Remove snapshots on the start day
    WHERE day != reward_start_exclusive
),

-- Step 7: Dedupe the active rewards by (avs, operator_set_id, snapshot, operator, reward_hash)
active_rewards_reduced_deduped AS (
    SELECT DISTINCT avs, operator_set_id, snapshot, operator, reward_hash
    FROM active_rewards_cleaned
),

-- Step 8: Divide by the number of snapshots that the operator was registered to an operator set for
op_set_op_num_registered_snapshots AS (
    SELECT
        ar.reward_hash,
        ar.operator,
        COUNT(*) AS num_registered_snapshots
    FROM active_rewards_reduced_deduped ar
    JOIN operator_set_operator_registration_snapshots osor
    ON
        ar.avs = osor.avs
        AND ar.operator_set_id = osor.operator_set_id
        AND ar.snapshot = osor.snapshot 
        AND ar.operator = osor.operator
    GROUP BY ar.reward_hash, ar.operator        
),

-- Step 9: Divide amount to pay by the number of snapshots that the operator was registered
active_rewards_with_registered_snapshots AS (
    SELECT
        arc.*,
        COALESCE(nrs.num_registered_snapshots, 0) as num_registered_snapshots
    FROM active_rewards_cleaned arc
    LEFT JOIN op_set_op_num_registered_snapshots nrs
    ON
        arc.reward_hash = nrs.reward_hash
        AND arc.operator = nrs.operator
),

-- Step 10: Divide amount to pay by the number of snapshots that the operator was registered
active_rewards_final AS (
    SELECT
        ar.*,
        CASE
            -- If the operator was not registered for any snapshots, just get regular tokens per day to refund the AVS
            WHEN ar.num_registered_snapshots = 0 THEN floor(ar.amount_decimal / (duration / 86400))
            ELSE floor(ar.amount_decimal / ar.num_registered_snapshots)
        END AS tokens_per_registered_snapshot_decimal
    FROM active_rewards_with_registered_snapshots ar
)

SELECT
    *,
    {{.generatedRewardsSnapshotId}} as generated_rewards_snapshot_id
FROM active_rewards_final
`

// Generate11GoldActiveODOperatorSetRewards generates active operator-directed rewards for the gold_11_active_od_operator_set_rewards table
//
// @param snapshotDate: The upper bound of when to calculate rewards to
// @param startDate: The lower bound of when to calculate rewards from. If we're running rewards for the first time,
// this will be "1970-01-01". If this is a subsequent run, this will be the last snapshot date.
func (r *RewardsCalculator) GenerateGold11ActiveODOperatorSetRewards(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	rewardsV2_1Enabled, err := r.globalConfig.IsRewardsV2_1EnabledForCutoffDate(snapshotDate)
	if err != nil {
		r.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}
	if !rewardsV2_1Enabled {
		r.logger.Sugar().Infow("Rewards v2.1 is not enabled for this cutoff date, skipping Generate11GoldActiveODOperatorSetRewards")
		return nil
	}

	destTableName := r.getTempActiveODOperatorSetRewardsTableName(snapshotDate, generatedRewardsSnapshotId)

	if err := r.DropTempActiveODOperatorSetRewardsTable(snapshotDate, generatedRewardsSnapshotId); err != nil {
		r.logger.Sugar().Errorw("Failed to drop temp active odos rewards table before copying", "error", err)
		return err
	}

	rewardsStart := "1970-01-01 00:00:00" // This will always start as this date and get's updated later in the query

	r.logger.Sugar().Infow("Generating active rewards",
		zap.String("rewardsStart", rewardsStart),
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_11_goldActiveODOperatorSetRewardsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"rewardsStart":               rewardsStart,
		"cutoffDate":                 snapshotDate,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
	})
	if err != nil {
		r.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := r.grm.Exec(query,
		sql.Named("cutoffDate", snapshotDate),
	)
	if res.Error != nil {
		r.logger.Sugar().Errorw("Failed to generate active od operator set rewards", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) getTempActiveODOperatorSetRewardsTableName(snapshotDate string, generatedRewardsSnapshotId uint64) string {
	camelDate := config.KebabToSnakeCase(snapshotDate)
	// shortened version to stay below the postgres 63 char limit
	return fmt.Sprintf("tmp_rewards_gold_11_active_odos_rewards_%s_%d", camelDate, generatedRewardsSnapshotId)
}

func (rc *RewardsCalculator) DropTempActiveODOperatorSetRewardsTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	tempTableName := rc.getTempActiveODOperatorSetRewardsTableName(snapshotDate, generatedRewardsSnapshotId)

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", tempTableName)
	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to drop temp active od operator set rewards table", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) CopyTempActiveODOperatorSetRewardsToActiveODOperatorSetRewards(snapshotDate string, generatedRewardsSnapshotId uint64) error {
	rc.logger.Sugar().Infow("Copying temp active od operator set rewards to active od operator set rewards",
		zap.String("snapshotDate", snapshotDate),
		zap.Uint64("generatedRewardsSnapshotId", generatedRewardsSnapshotId),
	)
	tempTableName := rc.getTempActiveODOperatorSetRewardsTableName(snapshotDate, generatedRewardsSnapshotId)
	destTableName := rewardsUtils.RewardsTable_11_ActiveODOperatorSetRewards

	query := `
		insert into {{.destTableName}} (avs, operator_set_id, operator, snapshot, token, amount_decimal, multiplier, strategy, duration, reward_hash, reward_submission_date, num_registered_snapshots, tokens_per_registered_snapshot_decimal, generated_rewards_snapshot_id)
		select avs, operator_set_id, operator, snapshot, token, amount_decimal, multiplier, strategy, duration, reward_hash, reward_submission_date, num_registered_snapshots, tokens_per_registered_snapshot_decimal, generated_rewards_snapshot_id from {{.tempTableName}}
		on conflict (reward_hash, operator_set_id, strategy, snapshot) do nothing
	`
	renderedQuery, err := rewardsUtils.RenderQueryTemplate(query, map[string]interface{}{
		"destTableName": destTableName,
		"tempTableName": tempTableName,
	})
	if err != nil {
		return fmt.Errorf("failed to render query template: %w", err)
	}
	res := rc.grm.Exec(renderedQuery)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to copy temp active OD rewards to active OD rewards", "error", res.Error)
		return res.Error
	}

	// return rc.DropTempActiveODOperatorSetRewardsTable(snapshotDate, generatedRewardsSnapshotId)
	return nil
}
