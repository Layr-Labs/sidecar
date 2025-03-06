package rpcServer

import (
	"context"
	slashingV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/slashing"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ListStakerSlashingHistory returns the slashing history for a given staker
func (s *RpcServer) ListStakerSlashingHistory(ctx context.Context, request *slashingV1.ListStakerSlashingHistoryRequest) (*slashingV1.ListStakerSlashingHistoryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// ListOperatorSlashingHistory returns the slashing history for a given operator
func (s *RpcServer) ListOperatorSlashingHistory(ctx context.Context, request *slashingV1.ListOperatorSlashingHistoryRequest) (*slashingV1.ListOperatorSlashingHistoryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// ListAvsSlashingHistory returns the slashing history for a given AVS
func (s *RpcServer) ListAvsSlashingHistory(ctx context.Context, request *slashingV1.ListAvsSlashingHistoryRequest) (*slashingV1.ListAvsSlashingHistoryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// ListAvsOperatorSetSlashingHistory returns the slashing history for a given AVS operator set
func (s *RpcServer) ListAvsOperatorSetSlashingHistory(ctx context.Context, request *slashingV1.ListAvsOperatorSetSlashingHistoryRequest) (*slashingV1.ListAvsOperatorSetSlashingHistoryResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
