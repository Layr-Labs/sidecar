package rpcServer

import (
	"context"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/storage"

	v1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type RpcServer struct {
	v1.UnimplementedRpcServer
	Logger            *zap.Logger
	blockStore        storage.BlockStore
	stateManager      *stateManager.EigenStateManager
	rewardsCalculator *rewards.RewardsCalculator
	rewardsQueue      *rewardsCalculatorQueue.RewardsCalculatorQueue
}

func NewRpcServer(
	ctx context.Context,
	grpcServer *grpc.Server,
	mux *runtime.ServeMux,
	bs storage.BlockStore,
	sm *stateManager.EigenStateManager,
	rc *rewards.RewardsCalculator,
	rcq *rewardsCalculatorQueue.RewardsCalculatorQueue,
	l *zap.Logger,
) (*RpcServer, error) {
	server := &RpcServer{
		blockStore:        bs,
		stateManager:      sm,
		rewardsCalculator: rc,
		rewardsQueue:      rcq,
		Logger:            l,
	}

	v1.RegisterHealthServer(grpcServer, server)
	if err := v1.RegisterHealthHandlerServer(ctx, mux, server); err != nil {
		l.Sugar().Errorw("Failed to register Health server", zap.Error(err))
		return nil, err
	}

	v1.RegisterRpcServer(grpcServer, server)
	if err := v1.RegisterRpcHandlerServer(ctx, mux, server); err != nil {
		l.Sugar().Errorw("Failed to register SidecarRpc server", zap.Error(err))
		return nil, err
	}

	v1.RegisterRewardsServer(grpcServer, server)
	if err := v1.RegisterRewardsHandlerServer(ctx, mux, server); err != nil {
		l.Sugar().Errorw("Failed to register Rewards server", zap.Error(err))
		return nil, err
	}

	return server, nil
}
