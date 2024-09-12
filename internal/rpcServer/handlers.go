package rpcServer

import (
	"context"

	v1 "github.com/Layr-Labs/go-sidecar/protos/eigenlayer/sidecar/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (rpc *RpcServer) GetBlockHeight(context.Context, *v1.GetBlockHeightRequest) (*v1.GetBlockHeightResponse, error) {
	block, err := rpc.blockStore.GetLatestBlock()

	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &v1.GetBlockHeightResponse{
		BlockNumber: block.Number,
		BlockHash:   block.Hash,
	}, nil
}

func (rpc *RpcServer) GetStateRoot(ctx context.Context, req *v1.GetStateRootRequest) (*v1.GetStateRootResponse, error) {
	blockNumber := req.GetBlockNumber()
	stateRoot, err := rpc.stateManager.GetStateRootForBlock(blockNumber)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &v1.GetStateRootResponse{
		EthBlockHash:   stateRoot.EthBlockHash,
		EthBlockNumber: stateRoot.EthBlockNumber,
		StateRoot:      stateRoot.StateRoot,
	}, nil
}

func (rpc *RpcServer) GenerateRewardAmounts(ctx context.Context, req *v1.GenerateRewardAmountsRequest) (*v1.GenerateRewardAmountsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GenerateRewardAmounts not implemented")
}
