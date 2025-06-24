package rpcServer

import (
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"testing"
	"time"

	v1EigenState "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/eigenState"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/avsOperators"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/completedSlashingWithdrawals"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/defaultOperatorSplits"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/disabledDistributionRoots"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/encumberedMagnitudes"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorAVSSplits"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorAllocationDelays"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorAllocations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorDirectedOperatorSetRewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorDirectedRewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorMaxMagnitudes"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorPISplits"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetOperatorRegistrations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetSplits"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetStrategyRegistrations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSets"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/queuedSlashingWithdrawals"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/rewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/slashedOperatorShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/slashedOperators"
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

		// Test with CompletedSlashingWithdrawal model
		t.Run("Completed Slashing Withdrawal Model", func(t *testing.T) {
			completedWithdrawal := &completedSlashingWithdrawals.CompletedSlashingWithdrawal{
				WithdrawalRoot:  "0xwithdrawal123",
				TransactionHash: "0xmno",
				LogIndex:        8,
				BlockNumber:     800,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				completedSlashingWithdrawals.CompletedSlashingWithdrawalModelName: {completedWithdrawal},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetCompletedSlashingWithdrawal()
			assert.NotNil(t, change)
			assert.Equal(t, "0xwithdrawal123", change.WithdrawalRoot)
			assert.Equal(t, "0xmno", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(8), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(800), change.TransactionMetadata.BlockHeight)
		})

		// Test with QueuedSlashingWithdrawal model
		t.Run("Queued Slashing Withdrawal Model", func(t *testing.T) {
			queuedWithdrawal := &queuedSlashingWithdrawals.QueuedSlashingWithdrawal{
				Staker:           "test_staker",
				Operator:         "test_operator",
				Withdrawer:       "test_withdrawer",
				Nonce:            "123",
				StartBlock:       900,
				Strategy:         "test_strategy",
				ScaledShares:     "5000",
				SharesToWithdraw: "4000",
				WithdrawalRoot:   "0xqueued456",
				TransactionHash:  "0xpqr",
				LogIndex:         9,
				BlockNumber:      900,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				queuedSlashingWithdrawals.QueuedSlashingWithdrawalModelName: {queuedWithdrawal},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetQueuedSlashingWithdrawal()
			assert.NotNil(t, change)
			assert.Equal(t, "test_operator", change.Operator)
			assert.Equal(t, "0xqueued456", change.WithdrawalRoot)
			assert.Equal(t, uint64(900), change.TargetBlock)
			assert.Equal(t, "0xpqr", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(9), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(900), change.TransactionMetadata.BlockHeight)
		})

		// Test with SlashedOperator model
		t.Run("Slashed Operator Model", func(t *testing.T) {
			slashedOperator := &slashedOperators.SlashedOperator{
				Operator:        "test_operator",
				OperatorSetId:   5,
				Avs:             "test_avs",
				TransactionHash: "0xstu",
				LogIndex:        10,
				BlockNumber:     1000,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				slashedOperators.SlashedOperatorModelName: {slashedOperator},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetSlashedOperator()
			assert.NotNil(t, change)
			assert.Equal(t, "test_operator", change.Operator)
			assert.Equal(t, uint64(5), change.OperatorSetId)
			assert.Equal(t, "test_avs", change.Avs)
			assert.Equal(t, "0xstu", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(10), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(1000), change.TransactionMetadata.BlockHeight)
		})

		// Test with SlashedOperatorShares model
		t.Run("Slashed Operator Shares Model", func(t *testing.T) {
			slashedShares := &slashedOperatorShares.SlashedOperatorShares{
				Operator:           "test_operator",
				Strategy:           "test_strategy",
				TotalSlashedShares: "3000",
				TransactionHash:    "0xvwx",
				LogIndex:           11,
				BlockNumber:        1100,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				slashedOperatorShares.SlashedOperatorSharesModelName: {slashedShares},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetSlashedOperatorShares()
			assert.NotNil(t, change)
			assert.Equal(t, "test_operator", change.Operator)
			assert.Equal(t, "test_strategy", change.Strategy)
			assert.Equal(t, "3000", change.Shares)
			assert.Equal(t, "0xvwx", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(11), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(1100), change.TransactionMetadata.BlockHeight)
		})

		// Test with DefaultOperatorSplit model
		t.Run("Default Operator Split Model", func(t *testing.T) {
			defaultSplit := &defaultOperatorSplits.DefaultOperatorSplit{
				OldDefaultOperatorSplitBips: 1000,
				NewDefaultOperatorSplitBips: 1500,
				TransactionHash:             "0yza",
				LogIndex:                    12,
				BlockNumber:                 1200,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				defaultOperatorSplits.DefaultOperatorSplitModelName: {defaultSplit},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetDefaultOperatorSplit()
			assert.NotNil(t, change)
			assert.Equal(t, uint64(1000), change.OldOperatorBasisPoints)
			assert.Equal(t, uint64(1500), change.NewOperatorBasisPoints)
			assert.Equal(t, "0yza", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(12), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(1200), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorAllocation model
		t.Run("Operator Allocation Model", func(t *testing.T) {
			allocation := &operatorAllocations.OperatorAllocation{
				Operator:        "test_operator",
				Strategy:        "test_strategy",
				Magnitude:       "10000",
				EffectiveBlock:  1300,
				OperatorSetId:   7,
				Avs:             "test_avs",
				TransactionHash: "0xbcd",
				LogIndex:        13,
				BlockNumber:     1300,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorAllocations.OperatorAllocationModelName: {allocation},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorAllocation()
			assert.NotNil(t, change)
			assert.Equal(t, "test_operator", change.Operator)
			assert.Equal(t, "test_strategy", change.Strategy)
			assert.Equal(t, "10000", change.Shares)
			assert.Equal(t, uint64(7), change.OperatorSetId)
			assert.Equal(t, "test_avs", change.Avs)
			assert.Equal(t, "0xbcd", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(13), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(1300), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorAllocationDelay model
		t.Run("Operator Allocation Delay Model", func(t *testing.T) {
			delay := &operatorAllocationDelays.OperatorAllocationDelay{
				Operator:        "test_operator",
				EffectiveBlock:  1400,
				Delay:           86400,
				TransactionHash: "0xefg",
				LogIndex:        14,
				BlockNumber:     1400,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorAllocationDelays.OperatorAllocationDelayModelName: {delay},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorAllocationDelay()
			assert.NotNil(t, change)
			assert.Equal(t, "test_operator", change.Operator)
			assert.Equal(t, uint64(1400), change.EffectiveBlock)
			assert.Equal(t, uint64(86400), change.Delay)
			assert.Equal(t, "0xefg", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(14), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(1400), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorAVSSplit model
		t.Run("Operator AVS Split Model", func(t *testing.T) {
			now := time.Now()

			avsSplit := &operatorAVSSplits.OperatorAVSSplit{
				Operator:                "test_operator",
				Avs:                     "test_avs",
				ActivatedAt:             &now,
				OldOperatorAVSSplitBips: 2000,
				NewOperatorAVSSplitBips: 2500,
				TransactionHash:         "0xhij",
				LogIndex:                15,
				BlockNumber:             1500,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorAVSSplits.OperatorAVSSplitModelName: {avsSplit},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorAvsSplit()
			assert.NotNil(t, change)
			assert.Equal(t, "test_operator", change.Operator)
			assert.Equal(t, "test_avs", change.Avs)
			assert.Equal(t, uint64(2000), change.OperatorBasisPoints)
			assert.Equal(t, uint64(2500), change.AvsBasisPoints)
			assert.Equal(t, "0xhij", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(15), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(1500), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorPISplit model
		t.Run("Operator PI Split Model", func(t *testing.T) {
			now := time.Now()

			piSplit := &operatorPISplits.OperatorPISplit{
				Operator:               "test_operator",
				ActivatedAt:            &now,
				OldOperatorPISplitBips: 500,
				NewOperatorPISplitBips: 750,
				TransactionHash:        "0xklm",
				LogIndex:               16,
				BlockNumber:            1600,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorPISplits.OperatorPISplitModelName: {piSplit},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorPiSplit()
			assert.NotNil(t, change)
			assert.Equal(t, "test_operator", change.Operator)
			assert.Equal(t, uint64(500), change.OperatorBasisPoints)
			assert.Equal(t, uint64(750), change.PiBasisPoints)
			assert.Equal(t, "0xklm", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(16), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(1600), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorSet model
		t.Run("Operator Set Model", func(t *testing.T) {
			operatorSet := &operatorSets.OperatorSet{
				OperatorSetId:   8,
				Avs:             "test_avs",
				TransactionHash: "0xnop",
				LogIndex:        17,
				BlockNumber:     1700,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorSets.OperatorSetModelName: {operatorSet},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorSet()
			assert.NotNil(t, change)
			assert.Equal(t, uint64(8), change.OperatorSetId)
			assert.Equal(t, "test_avs", change.Avs)
			assert.Equal(t, "0xnop", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(17), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(1700), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorSetOperatorRegistration model
		t.Run("Operator Set Operator Registration Model", func(t *testing.T) {
			registration := &operatorSetOperatorRegistrations.OperatorSetOperatorRegistration{
				Operator:        "test_operator",
				Avs:             "test_avs",
				OperatorSetId:   9,
				IsActive:        true,
				TransactionHash: "0xqrs",
				LogIndex:        18,
				BlockNumber:     1800,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorSetOperatorRegistrations.OperatorSetOperatorRegistrationModelName: {registration},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorSetOperatorRegistration()
			assert.NotNil(t, change)
			assert.Equal(t, "test_operator", change.Operator)
			assert.Equal(t, "test_avs", change.Avs)
			assert.Equal(t, uint64(9), change.OperatorSetId)
			assert.True(t, change.Registered)
			assert.Equal(t, "0xqrs", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(18), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(1800), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorSetSplit model
		t.Run("Operator Set Split Model", func(t *testing.T) {
			now := time.Now()

			operatorSetSplit := &operatorSetSplits.OperatorSetSplit{
				Operator:                "test_operator",
				Avs:                     "test_avs",
				OperatorSetId:           10,
				ActivatedAt:             &now,
				OldOperatorSetSplitBips: 3000,
				NewOperatorSetSplitBips: 3500,
				TransactionHash:         "0xtuv",
				LogIndex:                19,
				BlockNumber:             1900,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorSetSplits.OperatorSetSplitModelName: {operatorSetSplit},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorSetSplit()
			assert.NotNil(t, change)
			assert.Equal(t, uint64(10), change.OperatorSetId)
			assert.Equal(t, "test_avs", change.Avs)
			assert.Equal(t, uint64(3000), change.OperatorSetBasisPoints)
			assert.Equal(t, uint64(3500), change.AvsBasisPoints)
			assert.Equal(t, "0xtuv", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(19), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(1900), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorSetStrategyRegistration model
		t.Run("Operator Set Strategy Registration Model", func(t *testing.T) {
			strategyRegistration := &operatorSetStrategyRegistrations.OperatorSetStrategyRegistration{
				Strategy:        "test_strategy",
				Avs:             "test_avs",
				OperatorSetId:   11,
				IsActive:        true,
				TransactionHash: "0xwxy",
				LogIndex:        20,
				BlockNumber:     2000,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorSetStrategyRegistrations.OperatorSetStrategyRegistrationModelName: {strategyRegistration},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorSetStrategyRegistration()
			assert.NotNil(t, change)
			assert.Equal(t, "test_strategy", change.Strategy)
			assert.Equal(t, "test_avs", change.Avs)
			assert.Equal(t, uint64(11), change.OperatorSetId)
			assert.True(t, change.Registered)
			assert.Equal(t, "0xwxy", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(20), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(2000), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorDirectedRewardSubmission model
		t.Run("Operator Directed Reward Submission Model", func(t *testing.T) {
			now := time.Now()
			startTime := now.Add(-24 * time.Hour)
			endTime := now

			operatorReward := &operatorDirectedRewardSubmissions.OperatorDirectedRewardSubmission{
				Avs:             "test_avs",
				RewardHash:      "operator_reward_hash",
				Token:           "test_token",
				Operator:        "reward_operator",
				OperatorIndex:   1,
				Amount:          "7500",
				Strategy:        "test_strategy",
				StrategyIndex:   2,
				Multiplier:      "2.0",
				StartTimestamp:  &startTime,
				EndTimestamp:    &endTime,
				Duration:        86400,
				Description:     "Test operator reward",
				TransactionHash: "0xzab",
				LogIndex:        21,
				BlockNumber:     2100,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorDirectedRewardSubmissions.OperatorDirectedRewardSubmissionsModelName: {operatorReward},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorDirectedRewardSubmission()
			assert.NotNil(t, change)
			assert.Equal(t, "test_avs", change.Avs)
			assert.Equal(t, "operator_reward_hash", change.RewardHash)
			assert.Equal(t, "test_token", change.Token)
			assert.Equal(t, "7500", change.Amount)
			assert.Equal(t, "test_strategy", change.Strategy)
			assert.Equal(t, uint64(2), change.StrategyIndex)
			assert.Equal(t, "2.0", change.Multiplier)
			assert.Equal(t, uint64(86400), change.Duration)
			assert.Equal(t, "reward_operator", change.Recipient)
			assert.Equal(t, "0xzab", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(21), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(2100), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorDirectedOperatorSetRewardSubmission model
		t.Run("Operator Directed Operator Set Reward Submission Model", func(t *testing.T) {
			now := time.Now()
			startTime := now.Add(-12 * time.Hour)
			endTime := now

			operatorSetReward := &operatorDirectedOperatorSetRewardSubmissions.OperatorDirectedOperatorSetRewardSubmission{
				Avs:             "test_avs",
				OperatorSetId:   12,
				RewardHash:      "operator_set_reward_hash",
				Token:           "test_token",
				Operator:        "reward_operator",
				OperatorIndex:   3,
				Amount:          "9000",
				Strategy:        "test_strategy",
				StrategyIndex:   4,
				Multiplier:      "1.8",
				StartTimestamp:  &startTime,
				EndTimestamp:    &endTime,
				Duration:        43200,
				Description:     "Test operator set reward",
				TransactionHash: "0xcde",
				LogIndex:        22,
				BlockNumber:     2200,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorDirectedOperatorSetRewardSubmissions.OperatorDirectedOperatorSetRewardSubmissionsModelName: {operatorSetReward},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorDirectedOperatorSetRewardSubmission()
			assert.NotNil(t, change)
			assert.Equal(t, "test_avs", change.Avs)
			assert.Equal(t, "operator_set_reward_hash", change.RewardHash)
			assert.Equal(t, "test_token", change.Token)
			assert.Equal(t, "9000", change.Amount)
			assert.Equal(t, "test_strategy", change.Strategy)
			assert.Equal(t, uint64(4), change.StrategyIndex)
			assert.Equal(t, "1.8", change.Multiplier)
			assert.Equal(t, uint64(43200), change.Duration)
			assert.Equal(t, uint64(12), change.OperatorSetId)
			assert.Equal(t, "0xcde", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(22), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(2200), change.TransactionMetadata.BlockHeight)
		})

		// Test with EncumberedMagnitude model
		t.Run("Encumbered Magnitude Model", func(t *testing.T) {
			encumberedMagnitude := &encumberedMagnitudes.EncumberedMagnitude{
				Operator:            "test_operator",
				Strategy:            "test_strategy",
				EncumberedMagnitude: "15000",
				TransactionHash:     "0xfgh",
				LogIndex:            23,
				BlockNumber:         2300,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				encumberedMagnitudes.EncumberedMagnitudeModelName: {encumberedMagnitude},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetEncumberedMagnitude()
			assert.NotNil(t, change)
			assert.Equal(t, "test_operator", change.Operator)
			assert.Equal(t, "test_strategy", change.Strategy)
			assert.Equal(t, "15000", change.EncumberedMagnitude)
			assert.Equal(t, "0xfgh", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(23), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(2300), change.TransactionMetadata.BlockHeight)
		})

		// Test with OperatorMaxMagnitude model
		t.Run("Operator Max Magnitude Model", func(t *testing.T) {
			maxMagnitude := &operatorMaxMagnitudes.OperatorMaxMagnitude{
				Operator:        "test_operator",
				Strategy:        "test_strategy",
				MaxMagnitude:    "25000",
				TransactionHash: "0xijk",
				LogIndex:        24,
				BlockNumber:     2400,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				operatorMaxMagnitudes.OperatorMaxMagnitudeModelName: {maxMagnitude},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 1)

			change := result[0].GetOperatorMaxMagnitude()
			assert.NotNil(t, change)
			assert.Equal(t, "test_operator", change.Operator)
			assert.Equal(t, "test_strategy", change.Strategy)
			assert.Equal(t, "25000", change.MaxMagnitude)
			assert.Equal(t, "0xijk", change.TransactionMetadata.TransactionHash)
			assert.Equal(t, uint64(24), change.TransactionMetadata.LogIndex)
			assert.Equal(t, uint64(2400), change.TransactionMetadata.BlockHeight)
		})

		// Test with multiple models including new models
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

			// Add some new models to test
			slashedOperator := &slashedOperators.SlashedOperator{
				Operator:        "slashed_operator",
				OperatorSetId:   3,
				Avs:             "test_avs",
				TransactionHash: "0xslash",
				LogIndex:        25,
				BlockNumber:     2500,
			}

			operatorSet := &operatorSets.OperatorSet{
				OperatorSetId:   6,
				Avs:             "test_avs",
				TransactionHash: "0xopset",
				LogIndex:        26,
				BlockNumber:     2600,
			}

			result, err := rpc.parseCommittedChanges(map[string][]interface{}{
				avsOperators.AvsOperatorsModelName:           {avsOperator},
				stakerDelegations.StakerDelegationsModelName: {stakerDelegation},
				slashedOperators.SlashedOperatorModelName:    {slashedOperator},
				operatorSets.OperatorSetModelName:            {operatorSet},
			})

			assert.NoError(t, err)
			assert.Len(t, result, 4)

			// Check that we have one of each type
			var foundAvsOperator, foundStakerDelegation, foundSlashedOperator, foundOperatorSet bool

			for _, change := range result {
				if change.GetAvsOperatorStateChange() != nil {
					foundAvsOperator = true
				}
				if change.GetStakerDelegationChange() != nil {
					foundStakerDelegation = true
				}
				if change.GetSlashedOperator() != nil {
					foundSlashedOperator = true
				}
				if change.GetOperatorSet() != nil {
					foundOperatorSet = true
				}
			}

			assert.True(t, foundAvsOperator)
			assert.True(t, foundStakerDelegation)
			assert.True(t, foundSlashedOperator)
			assert.True(t, foundOperatorSet)
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
