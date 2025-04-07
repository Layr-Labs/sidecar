package rpcServer

import (
	"context"
	slashingV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/slashing"
	"github.com/Layr-Labs/sidecar/pkg/service/slashingDataService"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func convertSlashingEventToProtoSlashingEvent(slashingEvent *slashingDataService.SlashingEvent) *slashingV1.SlashingEvent {
	return &slashingV1.SlashingEvent{
		Description:     slashingEvent.Description,
		Operator:        slashingEvent.Operator,
		TransactionHash: slashingEvent.TransactionHash,
		LogIndex:        slashingEvent.LogIndex,
		BlockNumber:     slashingEvent.BlockNumber,
		Strategies: utils.Map(slashingEvent.Strategies, func(strategy *slashingDataService.Strategy, i uint64) *slashingV1.SlashingEvent_Strategy {
			return &slashingV1.SlashingEvent_Strategy{
				Strategy:           strategy.Strategy,
				WadSlashed:         strategy.WadSlashed,
				TotalSharesSlashed: strategy.TotalSharesSlashed,
			}
		}),
		OperatorSet: &slashingV1.SlashingEvent_OperatorSet{
			Id:  slashingEvent.OperatorSet.Id,
			Avs: slashingEvent.OperatorSet.Avs,
		},
	}
}

// ListStakerSlashingHistory returns the slashing history for a given staker
func (s *RpcServer) ListStakerSlashingHistory(ctx context.Context, request *slashingV1.ListStakerSlashingHistoryRequest) (*slashingV1.ListStakerSlashingHistoryResponse, error) {
	stakerAddress := request.GetStakerAddress()
	if stakerAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "staker address is required")
	}
	blockHeight := uint64(0)

	history, err := s.slashingDataService.ListStakerSlashingHistory(ctx, stakerAddress, blockHeight)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &slashingV1.ListStakerSlashingHistoryResponse{
		StakerSlashingEvents: utils.Map(history, func(slashingEvent *slashingDataService.SlashingEvent, i uint64) *slashingV1.StakerSlashingEvent {
			return &slashingV1.StakerSlashingEvent{
				Staker:        stakerAddress,
				SlashingEvent: convertSlashingEventToProtoSlashingEvent(slashingEvent),
			}
		}),
	}, nil
}

// ListOperatorSlashingHistory returns the slashing history for a given operator
func (s *RpcServer) ListOperatorSlashingHistory(ctx context.Context, request *slashingV1.ListOperatorSlashingHistoryRequest) (*slashingV1.ListOperatorSlashingHistoryResponse, error) {
	operatorAddress := request.GetOperatorAddress()
	if operatorAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "operator is required")
	}

	blockHeight := uint64(0)
	history, err := s.slashingDataService.ListOperatorSlashingHistory(ctx, operatorAddress, blockHeight)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &slashingV1.ListOperatorSlashingHistoryResponse{
		SlashingEvents: utils.Map(history, func(slashingEvent *slashingDataService.SlashingEvent, i uint64) *slashingV1.SlashingEvent {
			return convertSlashingEventToProtoSlashingEvent(slashingEvent)
		}),
	}, nil
}

// ListAvsSlashingHistory returns the slashing history for a given AVS
func (s *RpcServer) ListAvsSlashingHistory(ctx context.Context, request *slashingV1.ListAvsSlashingHistoryRequest) (*slashingV1.ListAvsSlashingHistoryResponse, error) {
	avsAddress := request.GetAvsAddress()
	if avsAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "avs is required")
	}

	blockHeight := uint64(0)
	history, err := s.slashingDataService.ListAvsSlashingHistory(ctx, avsAddress, blockHeight)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &slashingV1.ListAvsSlashingHistoryResponse{
		SlashingEvents: utils.Map(history, func(slashingEvent *slashingDataService.SlashingEvent, i uint64) *slashingV1.SlashingEvent {
			return convertSlashingEventToProtoSlashingEvent(slashingEvent)
		}),
	}, nil
}

// ListAvsOperatorSetSlashingHistory returns the slashing history for a given AVS operator set
func (s *RpcServer) ListAvsOperatorSetSlashingHistory(ctx context.Context, request *slashingV1.ListAvsOperatorSetSlashingHistoryRequest) (*slashingV1.ListAvsOperatorSetSlashingHistoryResponse, error) {
	avsAddress := request.GetAvsAddress()
	if avsAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "avs is required")
	}

	operatorSetId := request.GetOperatorSetId()

	blockHeight := uint64(0)
	history, err := s.slashingDataService.ListAvsOperatorSetSlashingHistory(ctx, avsAddress, operatorSetId, blockHeight)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &slashingV1.ListAvsOperatorSetSlashingHistoryResponse{
		SlashingEvents: utils.Map(history, func(slashingEvent *slashingDataService.SlashingEvent, i uint64) *slashingV1.SlashingEvent {
			return convertSlashingEventToProtoSlashingEvent(slashingEvent)
		}),
	}, nil
}
