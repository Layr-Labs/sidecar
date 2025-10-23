package rewards

import (
	"database/sql"
	"fmt"

	"go.uber.org/zap"
)

type UniqueStakeValidationError struct {
	Operator      string
	Avs           string
	OperatorSetId uint64
	Strategy      string
	Required      string
	Allocated     string
}

func (e *UniqueStakeValidationError) Error() string {
	return fmt.Sprintf("operator %s has insufficient unique stake allocated to AVS %s operator set %d for strategy %s: required %s, allocated %s",
		e.Operator, e.Avs, e.OperatorSetId, e.Strategy, e.Required, e.Allocated)
}

// ValidateUniqueStakeAllocationsForRewardSubmission validates that operators have allocated
// sufficient unique stake to be eligible for the reward submission
//
// This function checks that for each operator receiving rewards:
// 1. The operator has allocated unique stake to the relevant operator set
// 2. The allocated unique stake is greater than zero (slashable)
// 3. The operator is registered for the operator set at the snapshot date
func (rc *RewardsCalculator) ValidateUniqueStakeAllocationsForRewardSubmission(
	snapshotDate string,
	avs string,
	operatorSetId uint64,
	strategies []string,
	operators []string,
) error {
	rc.logger.Sugar().Infow("Validating unique stake allocations",
		zap.String("snapshotDate", snapshotDate),
		zap.String("avs", avs),
		zap.Uint64("operatorSetId", operatorSetId),
		zap.Strings("strategies", strategies),
		zap.Strings("operators", operators),
	)

	// CRITICAL: Get the block height AT the snapshot date for retroactive rewards support
	// This ensures we only validate allocations that were effective at that historical point in time
	type BlockAtDate struct {
		BlockNumber uint64
	}
	var blockAtDate BlockAtDate

	blockQuery := `
		SELECT number as block_number
		FROM blocks
		WHERE block_time::date <= @snapshotDate::date
		ORDER BY block_time DESC
		LIMIT 1
	`

	res := rc.grm.Raw(blockQuery, sql.Named("snapshotDate", snapshotDate)).Scan(&blockAtDate)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to get block at snapshot date", "error", res.Error)
		return res.Error
	}

	rc.logger.Sugar().Debugw("Validating unique stake allocations at block",
		zap.String("snapshotDate", snapshotDate),
		zap.Uint64("blockNumber", blockAtDate.BlockNumber),
	)

	// Query to check operator allocations
	query := `
		WITH latest_allocations AS (
			SELECT 
				operator,
				strategy,
				magnitude,
				avs,
				operator_set_id,
				effective_block,
				ROW_NUMBER() OVER (
					PARTITION BY operator, strategy, avs, operator_set_id 
					ORDER BY effective_block DESC, block_number DESC, log_index DESC
				) AS rn
			FROM operator_allocations
			WHERE 
				avs = @avs
				AND operator_set_id = @operatorSetId
				AND operator = ANY(@operators)
				AND strategy = ANY(@strategies)
				AND effective_block <= @cutoffBlockHeight
		),
		current_allocations AS (
			SELECT
				operator,
				strategy,
				magnitude,
				avs,
				operator_set_id
			FROM latest_allocations
			WHERE rn = 1
		),
		-- Check operator set registrations
		operator_registrations AS (
			SELECT DISTINCT
				operator,
				avs,
				operator_set_id
			FROM operator_set_operator_registration_snapshots osor
			WHERE 
				osor.avs = @avs
				AND osor.operator_set_id = @operatorSetId
				AND osor.snapshot = @snapshotDate
				AND osor.operator = ANY(@operators)
		)
		SELECT 
			o.operator,
			s.strategy,
			COALESCE(ca.magnitude, '0') as allocated_magnitude,
			CASE 
				WHEN or_reg.operator IS NOT NULL THEN true 
				ELSE false 
			END as is_registered
		FROM (SELECT unnest(@operators::text[]) as operator) o
		CROSS JOIN (SELECT unnest(@strategies::text[]) as strategy) s
		LEFT JOIN current_allocations ca 
			ON o.operator = ca.operator 
			AND s.strategy = ca.strategy
		LEFT JOIN operator_registrations or_reg
			ON o.operator = or_reg.operator
		ORDER BY o.operator, s.strategy
	`

	type AllocationResult struct {
		Operator           string
		Strategy           string
		AllocatedMagnitude string
		IsRegistered       bool
	}

	var results []AllocationResult
	res = rc.grm.Raw(query,
		sql.Named("avs", avs),
		sql.Named("operatorSetId", operatorSetId),
		sql.Named("operators", operators),
		sql.Named("strategies", strategies),
		sql.Named("cutoffBlockHeight", blockAtDate.BlockNumber),
		sql.Named("snapshotDate", snapshotDate),
	).Scan(&results)

	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to query unique stake allocations", "error", res.Error)
		return res.Error
	}

	// Validate results
	var validationErrors []error
	for _, result := range results {
		// Check if operator is registered
		if !result.IsRegistered {
			validationErrors = append(validationErrors, fmt.Errorf(
				"operator %s is not registered for AVS %s operator set %d at snapshot %s",
				result.Operator, avs, operatorSetId, snapshotDate))
			continue
		}

		// Check if allocation is greater than zero (slashable)
		if result.AllocatedMagnitude == "0" || result.AllocatedMagnitude == "" {
			validationErrors = append(validationErrors, &UniqueStakeValidationError{
				Operator:      result.Operator,
				Avs:           avs,
				OperatorSetId: operatorSetId,
				Strategy:      result.Strategy,
				Required:      "> 0",
				Allocated:     result.AllocatedMagnitude,
			})
		}
	}

	if len(validationErrors) > 0 {
		rc.logger.Sugar().Errorw("Unique stake validation failed",
			"errors", len(validationErrors),
			"snapshotDate", snapshotDate,
			"avs", avs,
			"operatorSetId", operatorSetId,
		)

		// Return the first validation error (you could also return all of them)
		return validationErrors[0]
	}

	rc.logger.Sugar().Infow("Unique stake validation passed",
		zap.String("snapshotDate", snapshotDate),
		zap.String("avs", avs),
		zap.Uint64("operatorSetId", operatorSetId),
		zap.Int("operatorsValidated", len(operators)),
		zap.Int("strategiesValidated", len(strategies)),
	)

	return nil
}

// ValidateUniqueStakeForRewardHash validates unique stake allocations for an existing reward submission
// by extracting the relevant parameters from the reward hash
func (rc *RewardsCalculator) ValidateUniqueStakeForRewardHash(rewardHash string, snapshotDate string) error {
	// Query to get reward submission details
	query := `
		SELECT DISTINCT
			avs,
			operator_set_id,
			ARRAY_AGG(DISTINCT strategy) as strategies,
			ARRAY_AGG(DISTINCT operator) as operators
		FROM (
			SELECT avs, NULL as operator_set_id, strategy, operator
			FROM operator_directed_rewards
			WHERE reward_hash = @rewardHash
			
			UNION ALL
			
			SELECT avs, operator_set_id, strategy, operator
			FROM operator_directed_operator_set_rewards
			WHERE reward_hash = @rewardHash
		) combined
		GROUP BY avs, operator_set_id
	`

	type RewardDetails struct {
		Avs           string
		OperatorSetId *uint64
		Strategies    []string
		Operators     []string
	}

	var details RewardDetails
	res := rc.grm.Raw(query, sql.Named("rewardHash", rewardHash)).Scan(&details)
	if res.Error != nil {
		return res.Error
	}

	// Only validate if this is an operator set reward (v2.2)
	if details.OperatorSetId != nil {
		return rc.ValidateUniqueStakeAllocationsForRewardSubmission(
			snapshotDate,
			details.Avs,
			*details.OperatorSetId,
			details.Strategies,
			details.Operators,
		)
	}

	return nil
}
