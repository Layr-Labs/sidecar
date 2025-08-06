package rpcServer

import (
	"context"
	"errors"

	operatorSetsV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/operatorSets"
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

	operators, err := rpc.protocolDataService.ListOperatorsForAvs(ctx, avsAddress, blockHeight)
	if err != nil {
		return nil, err
	}

	return &operatorSetsV1.ListOperatorsForAvsResponse{
		Operators: convertOperatorStringsToProto(operators),
	}, nil
}

func (rpc *RpcServer) ListOperatorsForBlockRange(ctx context.Context, request *operatorSetsV1.ListOperatorsForBlockRangeRequest) (*operatorSetsV1.ListOperatorsForBlockRangeResponse, error) {
	startBlock := request.GetStartBlock()
	endBlock := request.GetEndBlock()
	stakerAddress := request.GetStakerAddress()
	strategyAddress := request.GetStrategyAddress()
	avsAddress := request.GetAvsAddress()

	if startBlock == 0 || endBlock == 0 {
		return nil, errors.New("startBlock and endBlock are required")
	}

	if startBlock > endBlock {
		return nil, errors.New("startBlock cannot be greater than endBlock")
	}

	operators, err := rpc.protocolDataService.ListOperatorsForBlockRange(ctx, startBlock, endBlock, avsAddress, strategyAddress, stakerAddress)
	if err != nil {
		return nil, err
	}

	return &operatorSetsV1.ListOperatorsForBlockRangeResponse{
		Operators: convertOperatorStringsToProto(operators),
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
