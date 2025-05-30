package rewards

import (
	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _13_goldStakerODOperatorSetRewardAmountsQuery = `
INSERT INTO {{.destTableName}} (reward_hash, snapshot, token, tokens_per_registered_snapshot_decimal, avs, operator_set_id, operator, staker, strategy, multiplier, reward_submission_date, shares, staker_weight, rn, total_weight, staker_proportion, staker_tokens, generated_rewards_snapshot_id) AS

-- Step 1: Get the rows where operators have registered for the operator set
WITH reward_snapshot_operators AS (
    SELECT
        ap.reward_hash,
        ap.snapshot AS snapshot,
        ap.token,
        ap.tokens_per_registered_snapshot_decimal,
        ap.avs AS avs,
        ap.operator_set_id AS operator_set_id,
        ap.operator AS operator,
        ap.strategy,
        ap.multiplier,
        ap.reward_submission_date
    FROM {{.activeODRewardsTable}} ap
    JOIN operator_set_operator_registration_snapshots osor
        ON ap.avs = osor.avs 
       AND ap.operator_set_id = osor.operator_set_id
       AND ap.snapshot = osor.snapshot 
       AND ap.operator = osor.operator
),

-- Get the rows where strategies have registered for the operator set
operator_set_strategy_registrations AS (
    SELECT
        rso.*
    FROM reward_snapshot_operators rso
    JOIN operator_set_strategy_registration_snapshots ossr
        ON rso.avs = ossr.avs 
       AND rso.operator_set_id = ossr.operator_set_id
       AND rso.snapshot = ossr.snapshot
       AND rso.strategy = ossr.strategy
),

-- Calculate the total staker split for each operator reward with dynamic split logic
-- If no split is found, default to 1000 (10%)
staker_splits AS (
    SELECT 
        ossr.*,
        ossr.tokens_per_registered_snapshot_decimal - FLOOR(ossr.tokens_per_registered_snapshot_decimal * COALESCE(oss.split, dos.split, 1000) / CAST(10000 AS DECIMAL)) AS staker_split
    FROM operator_set_strategy_registrations ossr
    LEFT JOIN operator_set_split_snapshots oss
        ON ossr.operator = oss.operator 
       AND ossr.avs = oss.avs 
       AND ossr.operator_set_id = oss.operator_set_id
       AND ossr.snapshot = oss.snapshot
    LEFT JOIN default_operator_split_snapshots dos ON (ossr.snapshot = dos.snapshot)
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
staker_strategy_shares AS (
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
    FROM staker_strategy_shares
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
SELECT *, @generatedRewardsSnapshotId as generated_rewards_snapshot_id FROM staker_reward_amounts
`

func (rc *RewardsCalculator) GenerateGold13StakerODOperatorSetRewardAmountsTable(snapshotDate string, generatedSnapshotId uint64) error {
	rewardsV2_1Enabled, err := rc.globalConfig.IsRewardsV2_1EnabledForCutoffDate(snapshotDate)
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to check if rewards v2.1 is enabled", "error", err)
		return err
	}
	if !rewardsV2_1Enabled {
		rc.logger.Sugar().Infow("Rewards v2.1 is not enabled for this cutoff date, skipping GenerateGold13StakerODOperatorSetRewardAmountsTable")
		return nil
	}

	destTableName := rewardsUtils.RewardsTable_13_StakerODOperatorSetRewardAmounts

	rc.logger.Sugar().Infow("Generating Staker OD operator set reward amounts",
		zap.String("cutoffDate", snapshotDate),
		zap.String("destTableName", destTableName),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_13_goldStakerODOperatorSetRewardAmountsQuery, map[string]interface{}{
		"destTableName":              destTableName,
		"activeODRewardsTable":       rewardsUtils.RewardsTable_11_ActiveODOperatorSetRewards,
		"generatedRewardsSnapshotId": generatedSnapshotId,
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	query = query + " ON CONFLICT (reward_hash, snapshot, operator_set_id, operator, strategy) DO NOTHING"

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_staker_od_operator_set_reward_amounts", "error", res.Error)
		return res.Error
	}
	return nil
}
