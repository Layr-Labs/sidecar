package rpcServer

import (
	"context"
	"errors"
	sidecarV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (rpc *RpcServer) GetRewardsRoot(ctx context.Context, req *sidecarV1.GetRewardsRootRequest) (*sidecarV1.GetRewardsRootResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetRewardsRoot not implemented")
}

func (rpc *RpcServer) GenerateRewards(ctx context.Context, req *sidecarV1.GenerateRewardsRequest) (*sidecarV1.GenerateRewardsResponse, error) {
	cutoffDate := req.GetCutoffDate()

	var err error
	queued := false
	msg := rewardsCalculatorQueue.RewardsCalculationData{
		CalculationType: rewardsCalculatorQueue.RewardsCalculationType_CalculateRewards,
		CutoffDate:      cutoffDate,
	}
	data, qErr := rpc.rewardsQueue.EnqueueAndWait(ctx, msg)
	cutoffDate = data.CutoffDate
	err = qErr

	if err != nil {
		if errors.Is(err, &rewards.ErrRewardsCalculationInProgress{}) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &sidecarV1.GenerateRewardsResponse{
		CutoffDate: cutoffDate,
		Queued:     queued,
	}, nil
}

func (rpc *RpcServer) GenerateRewardsRoot(ctx context.Context, req *sidecarV1.GenerateRewardsRootRequest) (*sidecarV1.GenerateRewardsRootResponse, error) {
	cutoffDate := req.GetCutoffDate()
	if cutoffDate == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot date is required")
	}

	rpc.Logger.Sugar().Infow("Requesting rewards generation for snapshot date",
		zap.String("cutoffDate", cutoffDate),
	)
	_, err := rpc.rewardsQueue.EnqueueAndWait(context.Background(), rewardsCalculatorQueue.RewardsCalculationData{
		CalculationType: rewardsCalculatorQueue.RewardsCalculationType_CalculateRewards,
		CutoffDate:      cutoffDate,
	})
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	rpc.Logger.Sugar().Infow("Getting max snapshot for cutoff date",
		zap.String("cutoffDate", cutoffDate),
	)
	rewardsCalcEndDate, err := rpc.rewardsCalculator.GetMaxSnapshotDateForCutoffDate(cutoffDate)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if rewardsCalcEndDate == "" {
		return nil, status.Error(codes.NotFound, "no rewards calculated for the given snapshot date")
	}
	rpc.Logger.Sugar().Infow("Merkelizing rewards for snapshot date",
		zap.String("cutoffDate", cutoffDate),
		zap.String("rewardsCalcEndDate", rewardsCalcEndDate),
	)

	accountTree, _, err := rpc.rewardsCalculator.MerkelizeRewardsForSnapshot(rewardsCalcEndDate)
	if err != nil {
		rpc.Logger.Sugar().Errorw("failed to merkelize rewards for snapshot",
			zap.Error(err),
			zap.String("cutOffDate", cutoffDate),
			zap.String("rewardsCalcEndDate", rewardsCalcEndDate),
		)
		return nil, status.Error(codes.Internal, err.Error())
	}

	rootString := utils.ConvertBytesToString(accountTree.Root())
	rpc.Logger.Sugar().Infow("Rewards root generated",
		zap.String("root", rootString),
		zap.String("rewardsCalcEndDate", rewardsCalcEndDate),
		zap.String("cutoffDate", cutoffDate),
	)

	return &sidecarV1.GenerateRewardsRootResponse{
		RewardsRoot:        rootString,
		RewardsCalcEndDate: rewardsCalcEndDate,
	}, nil
}

func (rpc *RpcServer) GenerateStakerOperators(ctx context.Context, req *sidecarV1.GenerateStakerOperatorsRequest) (*sidecarV1.GenerateStakerOperatorsResponse, error) {
	cutoffDate := req.GetCutoffDate()

	if cutoffDate == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot date is required")
	}

	var err error
	queued := false
	msg := rewardsCalculatorQueue.RewardsCalculationData{
		CalculationType: rewardsCalculatorQueue.RewardsCalculationType_BackfillStakerOperatorsSnapshot,
		CutoffDate:      cutoffDate,
	}
	if req.GetWaitForComplete() {
		_, err = rpc.rewardsQueue.EnqueueAndWait(ctx, msg)
	} else {
		rpc.rewardsQueue.Enqueue(&rewardsCalculatorQueue.RewardsCalculationMessage{
			Data:         msg,
			ResponseChan: make(chan *rewardsCalculatorQueue.RewardsCalculatorResponse),
		})
		queued = true
	}

	if err != nil {
		if errors.Is(err, &rewards.ErrRewardsCalculationInProgress{}) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &sidecarV1.GenerateStakerOperatorsResponse{
		Queued: queued,
	}, nil
}

func (rpc *RpcServer) BackfillStakerOperators(ctx context.Context, req *sidecarV1.BackfillStakerOperatorsRequest) (*sidecarV1.BackfillStakerOperatorsResponse, error) {

	var err error
	queued := false
	msg := rewardsCalculatorQueue.RewardsCalculationData{
		CalculationType: rewardsCalculatorQueue.RewardsCalculationType_BackfillStakerOperators,
	}
	if req.GetWaitForComplete() {
		_, err = rpc.rewardsQueue.EnqueueAndWait(ctx, msg)
	} else {
		rpc.rewardsQueue.Enqueue(&rewardsCalculatorQueue.RewardsCalculationMessage{
			Data:         msg,
			ResponseChan: make(chan *rewardsCalculatorQueue.RewardsCalculatorResponse),
		})
		queued = true
	}

	if err != nil {
		if errors.Is(err, &rewards.ErrRewardsCalculationInProgress{}) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &sidecarV1.BackfillStakerOperatorsResponse{
		Queued: queued,
	}, nil
}

func (rpc *RpcServer) GetRewardsForSnapshot(ctx context.Context, req *sidecarV1.GetRewardsForSnapshotRequest) (*sidecarV1.GetRewardsForSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetRewardsForSnapshot not implemented")
}

func (rpc *RpcServer) GetAttributableRewardsForSnapshot(ctx context.Context, req *sidecarV1.GetAttributableRewardsForSnapshotRequest) (*sidecarV1.GetAttributableRewardsForSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetAttributableRewardsForSnapshot not implemented")
}

func (rpc *RpcServer) GetAttributableRewardsForDistributionRoot(ctx context.Context, req *sidecarV1.GetAttributableRewardsForDistributionRootRequest) (*sidecarV1.GetAttributableRewardsForDistributionRootResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetAttributableRewardsForDistributionRoot not implemented")
}

func (rpc *RpcServer) GenerateClaimProof(ctx context.Context, req *sidecarV1.GenerateClaimProofRequest) (*sidecarV1.GenerateClaimProofResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GenerateClaimProof not implemented")
}

func (rpc *RpcServer) GetAvailableRewards(ctx context.Context, req *sidecarV1.GetAvailableRewardsRequest) (*sidecarV1.GetAvailableRewardsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetAvailableRewards not implemented")
}

func (rpc *RpcServer) GetTotalClaimedRewards(ctx context.Context, req *sidecarV1.GetTotalClaimedRewardsRequest) (*sidecarV1.GetTotalClaimedRewardsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetTotalClaimedRewards not implemented")
}

func (rpc *RpcServer) GetAvailableRewardsTokens(ctx context.Context, req *sidecarV1.GetAvailableRewardsTokensRequest) (*sidecarV1.GetAvailableRewardsTokensResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetAvailableRewardsTokens not implemented")
}

func (rpc *RpcServer) GetSummarizedRewardsForEarner(ctx context.Context, req *sidecarV1.GetSummarizedRewardsForEarnerRequest) (*sidecarV1.GetSummarizedRewardsForEarnerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetSummarizedRewardsForEarner not implemented")
}

func (rpc *RpcServer) GetClaimedRewardsByBlock(ctx context.Context, req *sidecarV1.GetClaimedRewardsByBlockRequest) (*sidecarV1.GetClaimedRewardsByBlockResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetClaimedRewardsByBlock not implemented")
}
