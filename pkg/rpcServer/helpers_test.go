package rpcServer

import (
	"github.com/Layr-Labs/sidecar/internal/logger"
	"testing"
	"time"

	v1EigenState "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/eigenState"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/avsOperators"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/disabledDistributionRoots"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/rewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerDelegations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/submittedDistributionRoots"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/stretchr/testify/assert"
)

func Test_Helpers(t *testing.T) {
	t.Run("Parse Committed Changes", func(t *testing.T) {
		// Create a new RpcServer instance for testing
		logger, err := logger.NewLogger(&logger.LoggerConfig{Debug: false})
		assert.Nil(t, err)

		rpc := &RpcServer{
			Logger: logger,
		}

		// Test with empty input
		t.Run("Empty Input", func(t *testing.T) {
			result, err := rpc.parseCommittedChanges(map[string][]interface{}{})
			assert.NoError(t, err)
			assert.Empty(t, result)
		})

		// Test with unknown model name
		t.Run("Unknown Model", func(t *testing.T) {
			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				"unknown_model": {
					"some_data",
				},
			})
			assert.NoError(t, err)
			assert.Empty(t, result)
		})

		// Test with AvsOperator model
		t.Run("AVS Operators Model", func(t *testing.T) {
			avsOperator := &avsOperators.AvsOperatorStateChange{
				Avs:             "test_avs",
				Operator:        "test_operator",
				Registered:      true,
				TransactionHash: "0x123",
				LogIndex:        1,
				BlockNumber:     100,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				avsOperators.AvsOperatorsModelName: {avsOperator},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetAvsOperatorStateChange()
			assert.NotNil(t, change)
			assert.Equal(t, "test_avs", change.Avs)
			assert.Equal(t, "test_operator", change.Operator)
			assert.True(t, change.Registered)
			assert.Equal(t, "0x123", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(1), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(100), change.TransactionMetadata.BlockHeight)
		})

		// Test with DisabledDistributionRoot model
		t.Run("Disabled Distribution Roots Model", func(t *testing.T) {
			disabledRoot := &types.DisabledDistributionRoot{
				RootIndex:       42,
				TransactionHash: "0x456",
				LogIndex:        2,
				BlockNumber:     200,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				disabledDistributionRoots.DisabledDistributionRootsModelName: {disabledRoot},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetDisabledDistributionRoot()
			assert.NotNil(t, change)
			assert.Equal(t, uint64(42), change.RootIndex)
			assert.Equal(t, "0x456", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(2), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(200), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorShares model
		t.Run("Operator Shares Model", func(t *testing.T) {
			operatorShare := &operatorShares.OperatorShareDeltas{
				Operator:        "test_operator",
				Shares:          "1000",
				Strategy:        "test_strategy",
				Staker:          "test_staker",
				TransactionHash: "0x789",
				LogIndex:        3,
				BlockNumber:     300,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorShares.OperatorSharesModelName: {operatorShare},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorShareDelta()
			assert.NotNil(t, change)
			assert.Equal(t, "test_operator", change.Operator)
			assert.Equal(t, "1000", change.Shares)
			assert.Equal(t, "test_strategy", change.Strategy)
			assert.Equal(t, "test_staker", change.Staker)
			assert.Equal(t, "0x789", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(3), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(300), change.TransactionMetadata.BlockHeight)
		})

		// Test with RewardSubmissions model
		t.Run("Reward Submissions Model", func(t *testing.T) {
			now := time.Now()
			startTime := now.Add(-24 * time.Hour)
			endTime := now

			rewardSubmission := &rewardSubmissions.RewardSubmission{
				Avs:             "test_avs",
				RewardHash:      "reward_hash",
				Token:           "test_token",
				Amount:          "5000",
				Strategy:        "test_strategy",
				StrategyIndex:   1,
				Multiplier:      "1.5",
				StartTimestamp:  &startTime,
				EndTimestamp:    &endTime,
				Duration:        uint64(86400),
				RewardType:      "avs",
				TransactionHash: "0xabc",
				LogIndex:        4,
				BlockNumber:     400,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				rewardSubmissions.RewardSubmissionsModelName: {rewardSubmission},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetRewardSubmission()
			assert.NotNil(t, change)
			assert.Equal(t, "test_avs", change.Avs)
			assert.Equal(t, "reward_hash", change.RewardHash)
			assert.Equal(t, "test_token", change.Token)
			assert.Equal(t, "5000", change.Amount)
			assert.Equal(t, "test_strategy", change.Strategy)
			assert.Equal(t, uint64(1), change.StrategyIndex)
			assert.Equal(t, "1.5", change.Multiplier)
			assert.Equal(t, uint64(86400), change.Duration)
			assert.Equal(t, v1EigenState.RewardSubmission_AVS, change.RewardType)
			assert.Equal(t, "0xabc", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(4), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(400), change.TransactionMetadata.BlockHeight)
		})

		// Test with StakerDelegations model
		t.Run("Staker Delegations Model", func(t *testing.T) {
			stakerDelegation := &stakerDelegations.StakerDelegationChange{
				Staker:          "test_staker",
				Operator:        "test_operator",
				Delegated:       true,
				TransactionHash: "0xdef",
				LogIndex:        5,
				BlockNumber:     500,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				stakerDelegations.StakerDelegationsModelName: {stakerDelegation},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetStakerDelegationChange()
			assert.NotNil(t, change)
			assert.Equal(t, "test_staker", change.Staker)
			assert.Equal(t, "test_operator", change.Operator)
			assert.True(t, change.Delegated)
			assert.Equal(t, "0xdef", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(5), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(500), change.TransactionMetadata.BlockHeight)
		})

		// Test with StakerShares model
		t.Run("Staker Shares Model", func(t *testing.T) {
			now := time.Now()

			stakerShare := &stakerShares.StakerShareDeltas{
				Staker:          "test_staker",
				Strategy:        "test_strategy",
				Shares:          "2000",
				StrategyIndex:   2,
				BlockTime:       now,
				BlockDate:       now.Format("2006-01-02"),
				TransactionHash: "0xghi",
				LogIndex:        6,
				BlockNumber:     600,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				stakerShares.StakerSharesModelName: {stakerShare},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetStakerShareDelta()
			assert.NotNil(t, change)
			assert.Equal(t, "test_staker", change.Staker)
			assert.Equal(t, "test_strategy", change.Strategy)
			assert.Equal(t, "2000", change.Shares)
			assert.Equal(t, uint64(2), change.StrategyIndex)
			assert.Equal(t, now.Format("2006-01-02"), change.BlockDate)
			assert.Equal(t, "0xghi", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(6), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(600), change.TransactionMetadata.BlockHeight)
		})

		// Test with SubmittedDistributionRoot model
		t.Run("Submitted Distribution Roots Model", func(t *testing.T) {
			now := time.Now()

			submittedRoot := &types.SubmittedDistributionRoot{
				Root:                      "root_hash",
				RootIndex:                 7,
				RewardsCalculationEnd:     now.Add(-1 * time.Hour),
				RewardsCalculationEndUnit: "unit1",
				ActivatedAt:               now,
				ActivatedAtUnit:           "unit2",
				CreatedAtBlockNumber:      700,
				TransactionHash:           "0xjkl",
				LogIndex:                  7,
				BlockNumber:               700,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				submittedDistributionRoots.SubmittedDistributionRootsModelName: {submittedRoot},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetSubmittedDistributionRoot()
			assert.NotNil(t, change)
			assert.Equal(t, "root_hash", change.Root)
			assert.Equal(t, uint64(7), change.RootIndex)
			assert.Equal(t, "unit1", change.RewardsCalculationEndUnit)
			assert.Equal(t, "unit2", change.ActivatedAtUnit)
			assert.Equal(t, uint64(700), change.CreatedAtBlockNumber)
			assert.Equal(t, "0xjkl", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(7), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(700), change.TransactionMetadata.BlockHeight)
		})

		// Test with multiple models
		t.Run("Multiple Models", func(t *testing.T) {
			avsOperator := &avsOperators.AvsOperatorStateChange{
				Avs:             "test_avs",
				Operator:        "test_operator",
				Registered:      true,
				TransactionHash: "0x123",
				LogIndex:        1,
				BlockNumber:     100,
			}

			stakerDelegation := &stakerDelegations.StakerDelegationChange{
				Staker:          "test_staker",
				Operator:        "test_operator",
				Delegated:       true,
				TransactionHash: "0xdef",
				LogIndex:        5,
				BlockNumber:     500,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				avsOperators.AvsOperatorsModelName:           {avsOperator},
				stakerDelegations.StakerDelegationsModelName: {stakerDelegation},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 2)

			// Check that we have one of each type
			var foundAvsOperator, foundStakerDelegation bool

			for _, change := range result {
				if change.GetAvsOperatorStateChange() != nil {
					foundAvsOperator = true
				}
				if change.GetStakerDelegationChange() != nil {
					foundStakerDelegation = true
				}
			}

			assert.True(t, foundAvsOperator)
			assert.True(t, foundStakerDelegation)
		})
	})

	t.Run("Convert Reward Type To State Change", func(t *testing.T) {
		// Test valid reward types
		t.Run("Valid Reward Types", func(t *testing.T) {
			avsType, err := convertRewardTypeToStateChange("avs")
			assert.NoError(t, err)
			assert.Equal(t, v1EigenState.RewardSubmission_AVS, avsType)

			stakersType, err := convertRewardTypeToStateChange("all_stakers")
			assert.NoError(t, err)
			assert.Equal(t, v1EigenState.RewardSubmission_ALL_STAKERS, stakersType)

			earnersType, err := convertRewardTypeToStateChange("all_earners")
			assert.NoError(t, err)
			assert.Equal(t, v1EigenState.RewardSubmission_ALL_EARNERS, earnersType)
		})

		// Test invalid reward type
		t.Run("Invalid Reward Type", func(t *testing.T) {
			_, err := convertRewardTypeToStateChange("invalid_type")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "Invalid reward type")
		})
	})
}
