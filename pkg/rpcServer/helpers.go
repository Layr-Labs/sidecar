package rpcServer

import (
	"fmt"
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
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (rpc *RpcServer) parseCommittedChanges(committedStateByModel map[string][]interface{}) ([]*v1EigenState.EigenStateChange, error) {
	parsedChanges := make([]*v1EigenState.EigenStateChange, 0)

	for modelName, changes := range committedStateByModel {
		for _, change := range changes {
			switch modelName {
			case avsOperators.AvsOperatorsModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_AvsOperatorStateChange{
						AvsOperatorStateChange: convertAvsOperatorToStateChange(change),
					},
				})
			case disabledDistributionRoots.DisabledDistributionRootsModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_DisabledDistributionRoot{
						DisabledDistributionRoot: convertDisabledDistributionRootToStateChange(change),
					},
				})
			case operatorShares.OperatorSharesModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorShareDelta{
						OperatorShareDelta: convertOperatorSharesToStateChange(change),
					},
				})
			case rewardSubmissions.RewardSubmissionsModelName:
				parsedChange, err := convertRewardSubmissionToStateChange(change)
				if err != nil {
					return nil, err
				}

				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_RewardSubmission{
						RewardSubmission: parsedChange,
					},
				})
			case stakerDelegations.StakerDelegationsModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_StakerDelegationChange{
						StakerDelegationChange: convertStakerDelegationToStateChange(change),
					},
				})
			case stakerShares.StakerSharesModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_StakerShareDelta{
						StakerShareDelta: convertStakerSharesToStateChange(change),
					},
				})
			case submittedDistributionRoots.SubmittedDistributionRootsModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_SubmittedDistributionRoot{
						SubmittedDistributionRoot: convertSubmittedDistributionRootToStateChange(change),
					},
				})
			case completedSlashingWithdrawals.CompletedSlashingWithdrawalModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_CompletedSlashingWithdrawal{
						CompletedSlashingWithdrawal: convertCompletedSlashingWithdrawalToStateChange(change),
					},
				})
			case queuedSlashingWithdrawals.QueuedSlashingWithdrawalModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_QueuedSlashingWithdrawal{
						QueuedSlashingWithdrawal: convertQueuedSlashingWithdrawalToStateChange(change),
					},
				})
			case slashedOperators.SlashedOperatorModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_SlashedOperator{
						SlashedOperator: convertSlashedOperatorToStateChange(change),
					},
				})
			case slashedOperatorShares.SlashedOperatorSharesModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_SlashedOperatorShares{
						SlashedOperatorShares: convertSlashedOperatorSharesToStateChange(change),
					},
				})
			case defaultOperatorSplits.DefaultOperatorSplitModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_DefaultOperatorSplit{
						DefaultOperatorSplit: convertDefaultOperatorSplitToStateChange(change),
					},
				})
			case operatorAllocations.OperatorAllocationModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorAllocation{
						OperatorAllocation: convertOperatorAllocationToStateChange(change),
					},
				})
			case operatorAllocationDelays.OperatorAllocationDelayModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorAllocationDelay{
						OperatorAllocationDelay: convertOperatorAllocationDelayToStateChange(change),
					},
				})
			case operatorAVSSplits.OperatorAVSSplitModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorAvsSplit{
						OperatorAvsSplit: convertOperatorAVSSplitToStateChange(change),
					},
				})
			case operatorPISplits.OperatorPISplitModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorPiSplit{
						OperatorPiSplit: convertOperatorPISplitToStateChange(change),
					},
				})
			case operatorSets.OperatorSetModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorSet{
						OperatorSet: convertOperatorSetToStateChange(change),
					},
				})
			case operatorSetOperatorRegistrations.OperatorSetOperatorRegistrationModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorSetOperatorRegistration{
						OperatorSetOperatorRegistration: convertOperatorSetOperatorRegistrationToStateChange(change),
					},
				})
			case operatorSetSplits.OperatorSetSplitModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorSetSplit{
						OperatorSetSplit: convertOperatorSetSplitToStateChange(change),
					},
				})
			case operatorSetStrategyRegistrations.OperatorSetStrategyRegistrationModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorSetStrategyRegistration{
						OperatorSetStrategyRegistration: convertOperatorSetStrategyRegistrationToStateChange(change),
					},
				})
			case operatorDirectedRewardSubmissions.OperatorDirectedRewardSubmissionsModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorDirectedRewardSubmission{
						OperatorDirectedRewardSubmission: convertOperatorDirectedRewardSubmissionToStateChange(change),
					},
				})
			case operatorDirectedOperatorSetRewardSubmissions.OperatorDirectedOperatorSetRewardSubmissionsModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorDirectedOperatorSetRewardSubmission{
						OperatorDirectedOperatorSetRewardSubmission: convertOperatorDirectedOperatorSetRewardSubmissionToStateChange(change),
					},
				})
			case encumberedMagnitudes.EncumberedMagnitudeModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_EncumberedMagnitude{
						EncumberedMagnitude: convertEncumberedMagnitudeToStateChange(change),
					},
				})
			case operatorMaxMagnitudes.OperatorMaxMagnitudeModelName:
				parsedChanges = append(parsedChanges, &v1EigenState.EigenStateChange{
					Change: &v1EigenState.EigenStateChange_OperatorMaxMagnitude{
						OperatorMaxMagnitude: convertOperatorMaxMagnitudeToStateChange(change),
					},
				})
			default:
				rpc.Logger.Sugar().Debugw("Unknown model name", "modelName", modelName)
			}
		}
	}
	return parsedChanges, nil
}

func convertAvsOperatorToStateChange(change interface{}) *v1EigenState.AvsOperatorStateChange {
	typedChange := change.(*avsOperators.AvsOperatorStateChange)
	return &v1EigenState.AvsOperatorStateChange{
		Avs:        typedChange.Avs,
		Operator:   typedChange.Operator,
		Registered: typedChange.Registered,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertDisabledDistributionRootToStateChange(change interface{}) *v1EigenState.DisabledDistributionRoot {
	typedChange := change.(*types.DisabledDistributionRoot)
	return &v1EigenState.DisabledDistributionRoot{
		RootIndex: typedChange.RootIndex,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertOperatorSharesToStateChange(change interface{}) *v1EigenState.OperatorShareDelta {
	typedChange := change.(*operatorShares.OperatorShareDeltas)
	return &v1EigenState.OperatorShareDelta{
		Operator: typedChange.Operator,
		Shares:   typedChange.Shares,
		Strategy: typedChange.Strategy,
		Staker:   typedChange.Staker,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertRewardTypeToStateChange(rewardType string) (v1EigenState.RewardSubmission_RewardType, error) {
	switch rewardType {
	case "avs":
		return v1EigenState.RewardSubmission_AVS, nil
	case "all_stakers":
		return v1EigenState.RewardSubmission_ALL_STAKERS, nil
	case "all_earners":
		return v1EigenState.RewardSubmission_ALL_EARNERS, nil
	}
	return -1, fmt.Errorf("Invalid reward type '%s'", rewardType)
}

func convertRewardSubmissionToStateChange(change interface{}) (*v1EigenState.RewardSubmission, error) {
	typedChange := change.(*rewardSubmissions.RewardSubmission)
	rewardType, err := convertRewardTypeToStateChange(typedChange.RewardType)
	if err != nil {
		return nil, err
	}
	return &v1EigenState.RewardSubmission{
		Avs:            typedChange.Avs,
		RewardHash:     typedChange.RewardHash,
		Token:          typedChange.Token,
		Amount:         typedChange.Amount,
		Strategy:       typedChange.Strategy,
		StrategyIndex:  typedChange.StrategyIndex,
		Multiplier:     typedChange.Multiplier,
		StartTimestamp: timestamppb.New(*typedChange.StartTimestamp),
		EndTimestamp:   timestamppb.New(*typedChange.EndTimestamp),
		Duration:       typedChange.Duration,
		RewardType:     rewardType,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}, nil
}

func convertStakerDelegationToStateChange(change interface{}) *v1EigenState.StakerDelegationChange {
	typedChange := change.(*stakerDelegations.StakerDelegationChange)
	return &v1EigenState.StakerDelegationChange{
		Staker:    typedChange.Staker,
		Operator:  typedChange.Operator,
		Delegated: typedChange.Delegated,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertStakerSharesToStateChange(change interface{}) *v1EigenState.StakerShareDelta {
	typedChange := change.(*stakerShares.StakerShareDeltas)
	return &v1EigenState.StakerShareDelta{
		Staker:        typedChange.Staker,
		Strategy:      typedChange.Strategy,
		Shares:        typedChange.Shares,
		StrategyIndex: typedChange.StrategyIndex,
		BlockTime:     timestamppb.New(typedChange.BlockTime),
		BlockDate:     typedChange.BlockDate,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertSubmittedDistributionRootToStateChange(change interface{}) *v1EigenState.SubmittedDistributionRoot {
	typedChange := change.(*types.SubmittedDistributionRoot)
	return &v1EigenState.SubmittedDistributionRoot{
		Root:                      typedChange.Root,
		RootIndex:                 typedChange.RootIndex,
		RewardsCalculationEnd:     timestamppb.New(typedChange.RewardsCalculationEnd),
		RewardsCalculationEndUnit: typedChange.RewardsCalculationEndUnit,
		ActivatedAt:               timestamppb.New(typedChange.ActivatedAt),
		ActivatedAtUnit:           typedChange.ActivatedAtUnit,
		CreatedAtBlockNumber:      typedChange.CreatedAtBlockNumber,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			BlockHeight:     typedChange.BlockNumber,
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
		},
	}
}

// Slashing-related converter functions
func convertCompletedSlashingWithdrawalToStateChange(change interface{}) *v1EigenState.CompletedSlashingWithdrawal {
	typedChange := change.(*completedSlashingWithdrawals.CompletedSlashingWithdrawal)
	return &v1EigenState.CompletedSlashingWithdrawal{
		WithdrawalRoot: typedChange.WithdrawalRoot,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertQueuedSlashingWithdrawalToStateChange(change interface{}) *v1EigenState.QueuedSlashingWithdrawal {
	typedChange := change.(*queuedSlashingWithdrawals.QueuedSlashingWithdrawal)
	return &v1EigenState.QueuedSlashingWithdrawal{
		Operator:                 typedChange.Operator,
		WithdrawalRoot:           typedChange.WithdrawalRoot,
		TargetBlock:              typedChange.StartBlock,
		StakerOptOutWindowBlocks: 0,  // Field not available in struct
		OperatorSetId:            0,  // Field not available in struct
		Avs:                      "", // Field not available in struct
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertSlashedOperatorToStateChange(change interface{}) *v1EigenState.SlashedOperator {
	typedChange := change.(*slashedOperators.SlashedOperator)
	return &v1EigenState.SlashedOperator{
		Operator:      typedChange.Operator,
		OperatorSetId: typedChange.OperatorSetId,
		Avs:           typedChange.Avs,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertSlashedOperatorSharesToStateChange(change interface{}) *v1EigenState.SlashedOperatorShares {
	typedChange := change.(*slashedOperatorShares.SlashedOperatorShares)
	return &v1EigenState.SlashedOperatorShares{
		Operator: typedChange.Operator,
		Strategy: typedChange.Strategy,
		Shares:   typedChange.TotalSlashedShares,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

// Operator allocation & split converter functions
func convertDefaultOperatorSplitToStateChange(change interface{}) *v1EigenState.DefaultOperatorSplit {
	typedChange := change.(*defaultOperatorSplits.DefaultOperatorSplit)
	return &v1EigenState.DefaultOperatorSplit{
		Operator:               "", // Field not available in struct
		OldOperatorBasisPoints: typedChange.OldDefaultOperatorSplitBips,
		NewOperatorBasisPoints: typedChange.NewDefaultOperatorSplitBips,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertOperatorAllocationToStateChange(change interface{}) *v1EigenState.OperatorAllocation {
	typedChange := change.(*operatorAllocations.OperatorAllocation)
	return &v1EigenState.OperatorAllocation{
		Operator:      typedChange.Operator,
		OperatorSetId: typedChange.OperatorSetId,
		Avs:           typedChange.Avs,
		Strategy:      typedChange.Strategy,
		Shares:        typedChange.Magnitude,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertOperatorAllocationDelayToStateChange(change interface{}) *v1EigenState.OperatorAllocationDelay {
	typedChange := change.(*operatorAllocationDelays.OperatorAllocationDelay)
	return &v1EigenState.OperatorAllocationDelay{
		Operator:       typedChange.Operator,
		Delay:          typedChange.Delay,
		EffectiveBlock: typedChange.EffectiveBlock,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertOperatorAVSSplitToStateChange(change interface{}) *v1EigenState.OperatorAVSSplit {
	typedChange := change.(*operatorAVSSplits.OperatorAVSSplit)
	return &v1EigenState.OperatorAVSSplit{
		Operator:            typedChange.Operator,
		Avs:                 typedChange.Avs,
		OperatorBasisPoints: typedChange.OldOperatorAVSSplitBips,
		AvsBasisPoints:      typedChange.NewOperatorAVSSplitBips,
		StartTimestamp:      timestamppb.New(*typedChange.ActivatedAt),
		EndTimestamp:        timestamppb.New(*typedChange.ActivatedAt), // Same as start since we only have ActivatedAt
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertOperatorPISplitToStateChange(change interface{}) *v1EigenState.OperatorPISplit {
	typedChange := change.(*operatorPISplits.OperatorPISplit)
	return &v1EigenState.OperatorPISplit{
		Operator:            typedChange.Operator,
		OperatorBasisPoints: typedChange.OldOperatorPISplitBips,
		PiBasisPoints:       typedChange.NewOperatorPISplitBips,
		StartTimestamp:      timestamppb.New(*typedChange.ActivatedAt),
		EndTimestamp:        timestamppb.New(*typedChange.ActivatedAt), // Same as start since we only have ActivatedAt
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

// Operator set converter functions
func convertOperatorSetToStateChange(change interface{}) *v1EigenState.OperatorSet {
	typedChange := change.(*operatorSets.OperatorSet)
	return &v1EigenState.OperatorSet{
		OperatorSetId: typedChange.OperatorSetId,
		Avs:           typedChange.Avs,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertOperatorSetOperatorRegistrationToStateChange(change interface{}) *v1EigenState.OperatorSetOperatorRegistration {
	typedChange := change.(*operatorSetOperatorRegistrations.OperatorSetOperatorRegistration)
	return &v1EigenState.OperatorSetOperatorRegistration{
		Operator:      typedChange.Operator,
		OperatorSetId: typedChange.OperatorSetId,
		Avs:           typedChange.Avs,
		Registered:    typedChange.IsActive,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertOperatorSetSplitToStateChange(change interface{}) *v1EigenState.OperatorSetSplit {
	typedChange := change.(*operatorSetSplits.OperatorSetSplit)
	return &v1EigenState.OperatorSetSplit{
		OperatorSetId:          typedChange.OperatorSetId,
		Avs:                    typedChange.Avs,
		OperatorSetBasisPoints: typedChange.OldOperatorSetSplitBips,
		AvsBasisPoints:         typedChange.NewOperatorSetSplitBips,
		StartTimestamp:         timestamppb.New(*typedChange.ActivatedAt),
		EndTimestamp:           timestamppb.New(*typedChange.ActivatedAt), // Same as start since we only have ActivatedAt
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertOperatorSetStrategyRegistrationToStateChange(change interface{}) *v1EigenState.OperatorSetStrategyRegistration {
	typedChange := change.(*operatorSetStrategyRegistrations.OperatorSetStrategyRegistration)
	return &v1EigenState.OperatorSetStrategyRegistration{
		OperatorSetId: typedChange.OperatorSetId,
		Avs:           typedChange.Avs,
		Strategy:      typedChange.Strategy,
		Registered:    typedChange.IsActive,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

// Operator-directed reward converter functions
func convertOperatorDirectedRewardSubmissionToStateChange(change interface{}) *v1EigenState.OperatorDirectedRewardSubmission {
	typedChange := change.(*operatorDirectedRewardSubmissions.OperatorDirectedRewardSubmission)
	return &v1EigenState.OperatorDirectedRewardSubmission{
		Avs:            typedChange.Avs,
		RewardHash:     typedChange.RewardHash,
		Token:          typedChange.Token,
		Amount:         typedChange.Amount,
		Strategy:       typedChange.Strategy,
		StrategyIndex:  typedChange.StrategyIndex,
		Multiplier:     typedChange.Multiplier,
		StartTimestamp: timestamppb.New(*typedChange.StartTimestamp),
		EndTimestamp:   timestamppb.New(*typedChange.EndTimestamp),
		Duration:       typedChange.Duration,
		Recipient:      typedChange.Operator, // Use Operator field as recipient
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertOperatorDirectedOperatorSetRewardSubmissionToStateChange(change interface{}) *v1EigenState.OperatorDirectedOperatorSetRewardSubmission {
	typedChange := change.(*operatorDirectedOperatorSetRewardSubmissions.OperatorDirectedOperatorSetRewardSubmission)
	return &v1EigenState.OperatorDirectedOperatorSetRewardSubmission{
		Avs:            typedChange.Avs,
		RewardHash:     typedChange.RewardHash,
		Token:          typedChange.Token,
		Amount:         typedChange.Amount,
		Strategy:       typedChange.Strategy,
		StrategyIndex:  typedChange.StrategyIndex,
		Multiplier:     typedChange.Multiplier,
		StartTimestamp: timestamppb.New(*typedChange.StartTimestamp),
		EndTimestamp:   timestamppb.New(*typedChange.EndTimestamp),
		Duration:       typedChange.Duration,
		OperatorSetId:  typedChange.OperatorSetId,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

// Magnitude tracking converter functions
func convertEncumberedMagnitudeToStateChange(change interface{}) *v1EigenState.EncumberedMagnitude {
	typedChange := change.(*encumberedMagnitudes.EncumberedMagnitude)
	return &v1EigenState.EncumberedMagnitude{
		Operator:            typedChange.Operator,
		Strategy:            typedChange.Strategy,
		EncumberedMagnitude: typedChange.EncumberedMagnitude,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}

func convertOperatorMaxMagnitudeToStateChange(change interface{}) *v1EigenState.OperatorMaxMagnitude {
	typedChange := change.(*operatorMaxMagnitudes.OperatorMaxMagnitude)
	return &v1EigenState.OperatorMaxMagnitude{
		Operator:     typedChange.Operator,
		Strategy:     typedChange.Strategy,
		MaxMagnitude: typedChange.MaxMagnitude,
		TransactionMetadata: &v1EigenState.TransactionMetadata{
			TransactionHash: typedChange.TransactionHash,
			LogIndex:        typedChange.LogIndex,
			BlockHeight:     typedChange.BlockNumber,
		},
	}
}
