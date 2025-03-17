package rpcServer

import (
	"context"

	"github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/backfiller"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (rpc *RpcServer) BackfillBlocks(ctx context.Context, req *backfiller.BackfillBlocksRequest) (*backfiller.BackfillBlocksResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method BackfillBlocks not implemented")
}

func (rpc *RpcServer) GetBackfillStatus(ctx context.Context, req *backfiller.GetBackfillStatusRequest) (*backfiller.GetBackfillStatusResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetBackfillStatus not implemented")
}
