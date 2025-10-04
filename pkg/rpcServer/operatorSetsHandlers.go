package rpcServer

import (
	"context"
	"errors"

	operatorSetsV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/operatorSets"
	"github.com/Layr-Labs/sidecar/pkg/service/protocolDataService"
)

func (rpc *RpcServer) ListOperatorsForStaker(ctx context.Context, request *operatorSetsV1.ListOperatorsForStakerRequest) (*operatorSetsV1.ListOperatorsForStakerResponse, error) {
	stakerAddress := request.GetStakerAddress()
	blockHeight := request.GetBlockHeight()

	if stakerAddress == "" {
		return nil, errors.New("staker address is required")
	}

	operators, err := rpc.protocolDataService.ListOperatorsForStaker(ctx, stakerAddress, blockHeight)
	if err != nil {
		return nil, err
	}

	return &operatorSetsV1.ListOperatorsForStakerResponse{
		Operators: convertOperatorStringsToProto(operators),
	}, nil
}

func (rpc *RpcServer) ListOperatorsForStrategy(ctx context.Context, request *operatorSetsV1.ListOperatorsForStrategyRequest) (*operatorSetsV1.ListOperatorsForStrategyResponse, error) {
	strategyAddress := request.GetStrategyAddress()
	blockHeight := request.GetBlockHeight()

	if strategyAddress == "" {
		return nil, errors.New("strategy address is required")
	}

	operators, err := rpc.protocolDataService.ListOperatorsForStrategy(ctx, strategyAddress, blockHeight)
	if err != nil {
		return nil, err
	}

	return &operatorSetsV1.ListOperatorsForStrategyResponse{
		Operators: convertOperatorStringsToProto(operators),
	}, nil
}

func (rpc *RpcServer) ListOperatorsForAvs(ctx context.Context, request *operatorSetsV1.ListOperatorsForAvsRequest) (*operatorSetsV1.ListOperatorsForAvsResponse, error) {
	avsAddress := request.GetAvsAddress()
	blockHeight := request.GetBlockHeight()

	if avsAddress == "" {
		return nil, errors.New("avs address is required")
	}

	operatorRegistrations, err := rpc.protocolDataService.ListOperatorsForAvs(ctx, avsAddress, blockHeight)
	if err != nil {
		return nil, err
	}

	return &operatorSetsV1.ListOperatorsForAvsResponse{
		Operators: convertOperatorAvsRegistrationsToProto(operatorRegistrations),
	}, nil
}

func (rpc *RpcServer) ListOperatorSets(ctx context.Context, request *operatorSetsV1.ListOperatorSetsRequest) (*operatorSetsV1.ListOperatorSetsResponse, error) {
	operatorSets, err := rpc.protocolDataService.ListOperatorSets(ctx)
	if err != nil {
		return nil, err
	}

	return &operatorSetsV1.ListOperatorSetsResponse{
		OperatorSets: convertProtocolOperatorSetsToProto(operatorSets),
	}, nil
}

// convertOperatorStringsToProto converts operator address strings to protobuf Operator messages
func convertOperatorStringsToProto(operatorAddresses []string) []*operatorSetsV1.Operator {
	operators := make([]*operatorSetsV1.Operator, 0, len(operatorAddresses))

	for _, operatorAddress := range operatorAddresses {
		operators = append(operators, &operatorSetsV1.Operator{
			Operator: operatorAddress,
		})
	}

	return operators
}

// convertOperatorAvsRegistrationsToProto converts operator AVS registrations to protobuf Operator messages with operator set info
func convertOperatorAvsRegistrationsToProto(operatorSets []protocolDataService.OperatorSet) []*operatorSetsV1.Operator {
	operators := make([]*operatorSetsV1.Operator, 0, len(operatorSets))

	for _, operatorSet := range operatorSets {
		operator := &operatorSetsV1.Operator{
			Operator: operatorSet.Operator,
		}

		// Always populate the operator set info since OperatorSetId is uint64 (not pointer)
		operator.OperatorSets = []*operatorSetsV1.OperatorSet{
			{
				Id: operatorSet.OperatorSetId,
			},
		}

		operators = append(operators, operator)
	}

	return operators
}

// convertProtocolOperatorSetsToProto converts protocol operator sets to protobuf OperatorSet messages
func convertProtocolOperatorSetsToProto(operatorSets []protocolDataService.ProtocolOperatorSet) []*operatorSetsV1.OperatorSet {
	protoOperatorSets := make([]*operatorSetsV1.OperatorSet, 0, len(operatorSets))

	for _, operatorSet := range operatorSets {
		protoOperatorSets = append(protoOperatorSets, &operatorSetsV1.OperatorSet{
			Id:  operatorSet.Id,
			Avs: operatorSet.Avs,
		})
	}

	return protoOperatorSets
}
