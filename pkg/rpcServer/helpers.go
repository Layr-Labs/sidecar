package rpcServer

import (
	"fmt"
	v1EigenState "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/eigenState"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/avsOperators"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/disabledDistributionRoots"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/rewardSubmissions"
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
