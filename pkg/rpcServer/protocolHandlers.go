package rpcServer

import (
	"context"
	"errors"
	"github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/common"
	protocolV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/protocol"
	"github.com/Layr-Labs/sidecar/pkg/service/protocolDataService"
	"github.com/Layr-Labs/sidecar/pkg/service/types"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (rpc *RpcServer) GetRegisteredAvsForOperator(ctx context.Context, request *protocolV1.GetRegisteredAvsForOperatorRequest) (*protocolV1.GetRegisteredAvsForOperatorResponse, error) {
	operator := request.GetOperatorAddress()
	if operator == "" {
		return nil, errors.New("operator address is required")
	}

	blockHeight := request.GetBlockHeight()
	registeredAvs, err := rpc.protocolDataService.ListRegisteredAVSsForOperator(ctx, operator, blockHeight)
	if err != nil {
		return nil, err
	}

	return &protocolV1.GetRegisteredAvsForOperatorResponse{
		AvsAddresses: registeredAvs,
	}, nil
}

func (rpc *RpcServer) GetDelegatedStrategiesForOperator(ctx context.Context, request *protocolV1.GetDelegatedStrategiesForOperatorRequest) (*protocolV1.GetDelegatedStrategiesForOperatorResponse, error) {
	operator := request.GetOperatorAddress()
	blockHeight := request.GetBlockHeight()

	if operator == "" {
		return nil, errors.New("operator address is required")
	}

	delegatedStrategies, err := rpc.protocolDataService.ListDelegatedStrategiesForOperator(ctx, operator, blockHeight)
	if err != nil {
		return nil, err
	}

	return &protocolV1.GetDelegatedStrategiesForOperatorResponse{
		StrategyAddresses: delegatedStrategies,
	}, nil
}

func (rpc *RpcServer) GetOperatorDelegatedStakeForStrategy(ctx context.Context, request *protocolV1.GetOperatorDelegatedStakeForStrategyRequest) (*protocolV1.GetOperatorDelegatedStakeForStrategyResponse, error) {
	operator := request.GetOperatorAddress()
	strategy := request.GetStrategyAddress()
	blockHeight := request.GetBlockHeight()

	if operator == "" {
		return nil, errors.New("operator address is required")
	}

	if strategy == "" {
		return nil, errors.New("strategy address is required")
	}

	delegatedStake, err := rpc.protocolDataService.GetOperatorDelegatedStake(ctx, operator, strategy, blockHeight)

	if err != nil {
		return nil, err
	}

	return &protocolV1.GetOperatorDelegatedStakeForStrategyResponse{
		Shares:       delegatedStake.Shares,
		AvsAddresses: delegatedStake.AvsAddresses,
	}, nil
}

func (rpc *RpcServer) GetDelegatedStakersForOperator(ctx context.Context, request *protocolV1.GetDelegatedStakersForOperatorRequest) (*protocolV1.GetDelegatedStakersForOperatorResponse, error) {
	operator := request.GetOperatorAddress()
	if operator == "" {
		return nil, errors.New("operator address is required")
	}
	pagination := types.NewDefaultPagination()

	if p := request.GetPagination(); p != nil {
		pagination.Load(p.GetPageNumber(), p.GetPageSize())
	}

	delegatedStakers, err := rpc.protocolDataService.ListDelegatedStakersForOperator(ctx, operator, request.GetBlockHeight(), pagination)
	if err != nil {
		return nil, err
	}

	var nextPage *common.Pagination
	if uint32(len(delegatedStakers)) == pagination.PageSize {
		nextPage = &common.Pagination{
			PageNumber: pagination.Page + 1,
			PageSize:   pagination.PageSize,
		}
	}

	return &protocolV1.GetDelegatedStakersForOperatorResponse{
		StakerAddresses: delegatedStakers,
		NextPage:        nextPage,
	}, nil
}

func (rpc *RpcServer) GetStakerShares(ctx context.Context, request *protocolV1.GetStakerSharesRequest) (*protocolV1.GetStakerSharesResponse, error) {
	shares, err := rpc.protocolDataService.ListStakerShares(ctx, request.GetStakerAddress(), request.GetBlockHeight())
	if err != nil {
		return nil, err
	}

	stakerShares := make([]*protocolV1.StakerShare, 0, len(shares))
	for _, share := range shares {
		stakerShares = append(stakerShares, &protocolV1.StakerShare{
			Strategy:        share.Strategy,
			Shares:          share.Shares,
			OperatorAddress: share.Operator,
			AvsAddresses:    share.AvsAddresses,
		})
	}

	return &protocolV1.GetStakerSharesResponse{
		Shares: stakerShares,
	}, nil
}

func (rpc *RpcServer) GetEigenStateChanges(ctx context.Context, request *protocolV1.GetEigenStateChangesRequest) (*protocolV1.GetEigenStateChangesResponse, error) {
	stateChanges, err := rpc.protocolDataService.GetEigenStateChangesForBlock(ctx, request.GetBlockHeight())
	if err != nil {
		return nil, err
	}

	changes, err := rpc.parseCommittedChanges(stateChanges)
	if err != nil {
		return nil, err
	}

	return &protocolV1.GetEigenStateChangesResponse{
		Changes: changes,
	}, nil
}

// ListStrategies lists all strategies and includes the:
// - strategy address
// - total staked
// - list of tokens being rewarded
func (s *RpcServer) ListStrategies(ctx context.Context, request *protocolV1.ListStrategiesRequest) (*protocolV1.ListStrategiesResponse, error) {
	strategies, err := s.protocolDataService.ListStrategies(ctx, nil, 0)
	if err != nil {
		return nil, err
	}

	return &protocolV1.ListStrategiesResponse{
		Strategies: utils.Map(strategies, func(strategy *protocolDataService.Strategy, i uint64) *protocolV1.Strategy {
			return &protocolV1.Strategy{
				Strategy:       strategy.Strategy,
				TotalStake:     strategy.TotalStaked,
				RewardedTokens: strategy.RewardTokens,
			}
		}),
	}, nil
}

// ListStakerStrategies lists all strategies for a given staker and returns:
// - the strategy
// - the staked amount
func (s *RpcServer) ListStakerStrategies(ctx context.Context, request *protocolV1.ListStakerStrategiesRequest) (*protocolV1.ListStakerStrategiesResponse, error) {
	stakerAddress := request.GetStakerAddress()
	if stakerAddress == "" {
		return nil, errors.New("staker address is required")
	}
	blockHeight := request.GetBlockHeight()

	strategies, err := s.protocolDataService.ListStakerStrategies(ctx, stakerAddress, blockHeight, nil)
	if err != nil {
		return nil, err
	}

	return &protocolV1.ListStakerStrategiesResponse{
		StakerStrategies: utils.Map(strategies, func(strategy *protocolDataService.DetailedStakerStrategy, i uint64) *protocolV1.StakerStrategy {
			return &protocolV1.StakerStrategy{
				StakedAmount: strategy.Amount,
				Strategy: &protocolV1.Strategy{
					Strategy:       strategy.Strategy.Strategy,
					TotalStake:     strategy.Strategy.TotalStaked,
					RewardedTokens: strategy.Strategy.RewardTokens,
				},
			}
		}),
	}, nil
}

// GetStrategyForStaker returns the strategy for a given staker and includes the staked amount
func (s *RpcServer) GetStrategyForStaker(ctx context.Context, request *protocolV1.GetStrategyForStakerRequest) (*protocolV1.GetStrategyForStakerResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// ListStakerQueuedWithdrawals lists all queued withdrawals for a given staker and includes:
// - the strategy
// - the amount
// - the block height the withdrawal was queued at
func (s *RpcServer) ListStakerQueuedWithdrawals(ctx context.Context, request *protocolV1.ListStakerQueuedWithdrawalsRequest) (*protocolV1.ListStakerQueuedWithdrawalsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// ListStrategyQueuedWithdrawals lists all queued withdrawals for a given strategy and includes:
// - the staker
// - the amount
// - the block height the withdrawal was queued at
func (s *RpcServer) ListStrategyQueuedWithdrawals(ctx context.Context, request *protocolV1.ListStrategyQueuedWithdrawalsRequest) (*protocolV1.ListStrategyQueuedWithdrawalsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// ListOperatorQueuedWithdrawals lists all queued withdrawals by strategy and includes:
// - the staker
// - the amount
// - the block height the withdrawal was queued at
func (s *RpcServer) ListOperatorQueuedWithdrawals(ctx context.Context, request *protocolV1.ListOperatorQueuedWithdrawalsRequest) (*protocolV1.ListOperatorQueuedWithdrawalsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

// ListOperatorStrategyQueuedWithdrawals lists all queued withdrawals for a given strategy and includes:
// - the staker
// - the amount
// - the block height the withdrawal was queued at
func (s *RpcServer) ListOperatorStrategyQueuedWithdrawals(ctx context.Context, request *protocolV1.ListOperatorStrategyQueuedWithdrawalsRequest) (*protocolV1.ListOperatorStrategyQueuedWithdrawalsResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}
