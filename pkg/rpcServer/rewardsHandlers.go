package rpcServer

import (
	"context"
	"errors"
	"fmt"
	rewardsV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/rewards"
	"github.com/Layr-Labs/sidecar/pkg/metaState/types"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/service/rewardsDataService"
	serviceTypes "github.com/Layr-Labs/sidecar/pkg/service/types"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"slices"
	"strings"
)

func (rpc *RpcServer) GetRewardsRoot(ctx context.Context, req *rewardsV1.GetRewardsRootRequest) (*rewardsV1.GetRewardsRootResponse, error) {
	blockHeight := req.GetBlockHeight()

	root, err := rpc.rewardsDataService.GetDistributionRootForBlockHeight(ctx, blockHeight)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if root == nil {
		return nil, status.Error(codes.NotFound, "no rewards root found for the given block height")
	}
	return &rewardsV1.GetRewardsRootResponse{
		RewardsRoot: &rewardsV1.DistributionRoot{
			Root:                      root.Root,
			RootIndex:                 root.RootIndex,
			RewardsCalculationEnd:     timestamppb.New(root.RewardsCalculationEnd),
			RewardsCalculationEndUnit: root.RewardsCalculationEndUnit,
			ActivatedAt:               timestamppb.New(root.ActivatedAt),
			ActivatedAtUnit:           root.ActivatedAtUnit,
			CreatedAtBlockNumber:      root.CreatedAtBlockNumber,
			TransactionHash:           root.TransactionHash,
			BlockHeight:               root.BlockNumber,
			LogIndex:                  root.LogIndex,
			Disabled:                  root.Disabled,
		},
	}, nil
}

func (rpc *RpcServer) GenerateRewards(ctx context.Context, req *rewardsV1.GenerateRewardsRequest) (*rewardsV1.GenerateRewardsResponse, error) {
	// Generating rewards is a write operation so we need to proxy the request to the "primary" sidecar
	if !rpc.globalConfig.SidecarPrimaryConfig.IsPrimary {
		return rpc.sidecarClient.RewardsClient.GenerateRewards(ctx, req)
	}

	cutoffDate := req.GetCutoffDate()
	waitForComplete := req.GetWaitForComplete()

	var err error
	queued := false
	msg := rewardsCalculatorQueue.RewardsCalculationData{
		CalculationType: rewardsCalculatorQueue.RewardsCalculationType_CalculateRewards,
		CutoffDate:      cutoffDate,
	}

	if waitForComplete {
		data, qErr := rpc.rewardsQueue.EnqueueAndWait(ctx, msg)
		cutoffDate = data.CutoffDate
		err = qErr
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
	return &rewardsV1.GenerateRewardsResponse{
		CutoffDate: cutoffDate,
		Queued:     queued,
	}, nil
}

func (rpc *RpcServer) GenerateRewardsRoot(ctx context.Context, req *rewardsV1.GenerateRewardsRootRequest) (*rewardsV1.GenerateRewardsRootResponse, error) {
	// Generating rewards is a write operation so we need to proxy the request to the "primary" sidecar
	if !rpc.globalConfig.SidecarPrimaryConfig.IsPrimary {
		return rpc.sidecarClient.RewardsClient.GenerateRewardsRoot(ctx, req)
	}
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

	accountTree, _, _, err := rpc.rewardsCalculator.MerkelizeRewardsForSnapshot(rewardsCalcEndDate)
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

	return &rewardsV1.GenerateRewardsRootResponse{
		RewardsRoot:        rootString,
		RewardsCalcEndDate: rewardsCalcEndDate,
	}, nil
}

func (rpc *RpcServer) GenerateStakerOperators(ctx context.Context, req *rewardsV1.GenerateStakerOperatorsRequest) (*rewardsV1.GenerateStakerOperatorsResponse, error) {
	// Generating the staker operators table involves writing to the database so we must proxy to
	// the primary sidecar instance
	if !rpc.globalConfig.SidecarPrimaryConfig.IsPrimary {
		return rpc.sidecarClient.RewardsClient.GenerateStakerOperators(ctx, req)
	}
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
	return &rewardsV1.GenerateStakerOperatorsResponse{
		Queued: queued,
	}, nil
}

func (rpc *RpcServer) BackfillStakerOperators(ctx context.Context, req *rewardsV1.BackfillStakerOperatorsRequest) (*rewardsV1.BackfillStakerOperatorsResponse, error) {
	// Backfilling the staker operators table involves writing to the database so we must proxy to
	// the primary sidecar instance
	if !rpc.globalConfig.SidecarPrimaryConfig.IsPrimary {
		return rpc.sidecarClient.RewardsClient.BackfillStakerOperators(ctx, req)
	}
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
	return &rewardsV1.BackfillStakerOperatorsResponse{
		Queued: queued,
	}, nil
}

func (rpc *RpcServer) GetRewardsForSnapshot(ctx context.Context, req *rewardsV1.GetRewardsForSnapshotRequest) (*rewardsV1.GetRewardsForSnapshotResponse, error) {
	snapshot := req.GetSnapshot()
	if snapshot == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot is required")
	}

	earner := req.GetEarner()

	earners := make([]string, 0)
	if earner != "" {
		earners = append(earners, earner)
	}

	snapshotRewards, err := rpc.rewardsDataService.GetRewardsForSnapshot(ctx, snapshot, earners)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	rewardsRes := make([]*rewardsV1.Reward, 0, len(snapshotRewards))

	for _, reward := range snapshotRewards {
		rewardsRes = append(rewardsRes, &rewardsV1.Reward{
			Earner:   reward.Earner,
			Amount:   reward.CumulativeAmount,
			Snapshot: reward.Snapshot,
			Token:    reward.Token,
		})
	}

	return &rewardsV1.GetRewardsForSnapshotResponse{
		Rewards: rewardsRes,
	}, nil
}

func (rpc *RpcServer) GetRewardsForDistributionRoot(ctx context.Context, req *rewardsV1.GetRewardsForDistributionRootRequest) (*rewardsV1.GetRewardsForDistributionRootResponse, error) {
	rootIndex := req.GetRootIndex()

	snapshotRewards, err := rpc.rewardsDataService.GetRewardsForDistributionRoot(ctx, rootIndex)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	rewardsRes := make([]*rewardsV1.Reward, 0, len(snapshotRewards))

	for _, reward := range snapshotRewards {
		rewardsRes = append(rewardsRes, &rewardsV1.Reward{
			Earner:   reward.Earner,
			Amount:   reward.CumulativeAmount,
			Snapshot: reward.Snapshot,
			Token:    reward.Token,
		})
	}

	return &rewardsV1.GetRewardsForDistributionRootResponse{
		Rewards: rewardsRes,
	}, nil
}

func (rpc *RpcServer) GetAttributableRewardsForSnapshot(ctx context.Context, req *rewardsV1.GetAttributableRewardsForSnapshotRequest) (*rewardsV1.GetAttributableRewardsForSnapshotResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetAttributableRewardsForSnapshot not implemented")
}

func (rpc *RpcServer) GetAttributableRewardsForDistributionRoot(ctx context.Context, req *rewardsV1.GetAttributableRewardsForDistributionRootRequest) (*rewardsV1.GetAttributableRewardsForDistributionRootResponse, error) {
	return nil, status.Error(codes.Unimplemented, "method GetAttributableRewardsForDistributionRoot not implemented")
}

func (rpc *RpcServer) GetClaimableRewards(ctx context.Context, req *rewardsV1.GetClaimableRewardsRequest) (*rewardsV1.GetClaimableRewardsResponse, error) {
	earner := req.GetEarnerAddress()
	blockHeight := req.GetBlockHeight()

	if earner == "" {
		return nil, status.Error(codes.InvalidArgument, "earner address is required")
	}

	claimableRewards, snapshot, err := rpc.rewardsDataService.GetClaimableRewardsForEarner(ctx, earner, nil, blockHeight)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &rewardsV1.GetClaimableRewardsResponse{
		Rewards: utils.Map(claimableRewards, func(r *rewardsDataService.RewardAmount, i uint64) *rewardsV1.Reward {
			return &rewardsV1.Reward{
				Earner:   earner,
				Token:    r.Token,
				Amount:   r.Amount,
				Snapshot: snapshot.GetSnapshotDate(),
			}
		}),
	}, nil
}

// GetTotalClaimedRewards returns the total claimed rewards for an earner up to, and including, the provided block height.
func (rpc *RpcServer) GetTotalClaimedRewards(ctx context.Context, req *rewardsV1.GetTotalClaimedRewardsRequest) (*rewardsV1.GetTotalClaimedRewardsResponse, error) {
	earner := req.GetEarnerAddress()
	blockHeight := req.GetBlockHeight()

	if earner == "" {
		return nil, status.Error(codes.InvalidArgument, "earner address is required")
	}

	totalClaimed, err := rpc.rewardsDataService.GetTotalClaimedRewards(ctx, earner, nil, blockHeight)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &rewardsV1.GetTotalClaimedRewardsResponse{
		Rewards: utils.Map(totalClaimed, func(r *rewardsDataService.TotalClaimedReward, i uint64) *rewardsV1.TotalClaimedReward {
			return &rewardsV1.TotalClaimedReward{
				Earner: r.Earner,
				Token:  r.Token,
				Amount: r.Amount,
			}
		}),
	}, nil
}

// GetAvailableRewardsTokens returns the available rewards tokens for an earner at the provided block height. Just the tokens, not the
func (rpc *RpcServer) GetAvailableRewardsTokens(ctx context.Context, req *rewardsV1.GetAvailableRewardsTokensRequest) (*rewardsV1.GetAvailableRewardsTokensResponse, error) {
	earner := req.GetEarnerAddress()
	blockHeight := req.GetBlockHeight()

	if earner == "" {
		return nil, status.Error(codes.InvalidArgument, "earner address is required")
	}

	tokens, err := rpc.rewardsDataService.ListAvailableRewardsTokens(ctx, earner, blockHeight)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &rewardsV1.GetAvailableRewardsTokensResponse{
		Tokens: tokens,
	}, nil
}

func withDefaultValue(value string, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}

func (rpc *RpcServer) GetSummarizedRewardsForEarner(ctx context.Context, req *rewardsV1.GetSummarizedRewardsForEarnerRequest) (*rewardsV1.GetSummarizedRewardsForEarnerResponse, error) {
	earner := req.GetEarnerAddress()
	blockHeight := req.GetBlockHeight()

	if earner == "" {
		return nil, status.Error(codes.InvalidArgument, "earner address is required")
	}

	summarizedRewards, err := rpc.rewardsDataService.GetSummarizedRewards(ctx, earner, nil, blockHeight)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &rewardsV1.GetSummarizedRewardsForEarnerResponse{
		Rewards: utils.Map(summarizedRewards, func(r *rewardsDataService.SummarizedReward, i uint64) *rewardsV1.SummarizedEarnerReward {
			return &rewardsV1.SummarizedEarnerReward{
				Token:     r.Token,
				Earned:    withDefaultValue(r.Earned, "0"),
				Active:    withDefaultValue(r.Active, "0"),
				Claimed:   withDefaultValue(r.Claimed, "0"),
				Claimable: withDefaultValue(r.Claimable, "0"),
			}
		}),
	}, nil
}

// GetClaimedRewardsByBlock returns the claimed rewards for an earner for a specific block.
func (rpc *RpcServer) GetClaimedRewardsByBlock(ctx context.Context, req *rewardsV1.GetClaimedRewardsByBlockRequest) (*rewardsV1.GetClaimedRewardsByBlockResponse, error) {
	blockHeight := req.GetBlockHeight()

	claims, err := rpc.rewardsDataService.ListClaimedRewardsByBlockRange(ctx, "", blockHeight, blockHeight, nil)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &rewardsV1.GetClaimedRewardsByBlockResponse{
		Rewards: utils.Map(claims, func(c *types.RewardsClaimed, i uint64) *rewardsV1.ClaimedReward {
			return &rewardsV1.ClaimedReward{
				Earner:           c.Earner,
				Claimer:          c.Claimer,
				Token:            c.Token,
				BlockNumber:      c.BlockNumber,
				Recipient:        c.Recipient,
				DistributionRoot: c.Root,
				Amount:           c.ClaimedAmount,
			}
		}),
	}, nil
}

// ListClaimedRewardsByBlockRange returns the claimed rewards for each block in the given range (inclusive of start and end block heights).
func (rpc *RpcServer) ListClaimedRewardsByBlockRange(ctx context.Context, req *rewardsV1.ListClaimedRewardsByBlockRangeRequest) (*rewardsV1.ListClaimedRewardsByBlockRangeResponse, error) {
	earner := req.GetEarnerAddress()
	startBlockHeight := req.GetStartBlockHeight()
	endBlockHeight := req.GetEndBlockHeight()

	if earner == "" {
		return nil, status.Error(codes.InvalidArgument, "earner address is required")
	}

	claims, err := rpc.rewardsDataService.ListClaimedRewardsByBlockRange(ctx, earner, startBlockHeight, endBlockHeight, nil)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &rewardsV1.ListClaimedRewardsByBlockRangeResponse{
		Rewards: utils.Map(claims, func(c *types.RewardsClaimed, i uint64) *rewardsV1.ClaimedReward {
			return &rewardsV1.ClaimedReward{
				Earner:           c.Earner,
				Claimer:          c.Claimer,
				Token:            c.Token,
				BlockNumber:      c.BlockNumber,
				Recipient:        c.Recipient,
				DistributionRoot: c.Root,
				Amount:           c.ClaimedAmount,
			}
		}),
	}, nil
}

func (rpc *RpcServer) ListDistributionRoots(ctx context.Context, req *rewardsV1.ListDistributionRootsRequest) (*rewardsV1.ListDistributionRootsResponse, error) {
	blockHeight := req.GetBlockHeight()
	roots, err := rpc.rewardsCalculator.ListDistributionRoots(blockHeight)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	responseRoots := make([]*rewardsV1.DistributionRoot, 0, len(roots))
	for _, root := range roots {
		responseRoots = append(responseRoots, &rewardsV1.DistributionRoot{
			Root:                      root.Root,
			RootIndex:                 root.RootIndex,
			RewardsCalculationEnd:     timestamppb.New(root.RewardsCalculationEnd),
			RewardsCalculationEndUnit: root.RewardsCalculationEndUnit,
			ActivatedAt:               timestamppb.New(root.ActivatedAt),
			ActivatedAtUnit:           root.ActivatedAtUnit,
			CreatedAtBlockNumber:      root.CreatedAtBlockNumber,
			TransactionHash:           root.TransactionHash,
			BlockHeight:               root.BlockNumber,
			LogIndex:                  root.LogIndex,
			Disabled:                  root.Disabled,
		})
	}

	return &rewardsV1.ListDistributionRootsResponse{
		DistributionRoots: responseRoots,
	}, nil
}

func convertRewardTypeToEnum(rewardType string) (rewardsV1.RewardType, error) {
	switch rewardType {
	case "avs":
		return rewardsV1.RewardType_REWARD_TYPE_AVS, nil
	case "all_stakers":
		return rewardsV1.RewardType_REWARD_TYPE_FOR_ALL, nil
	case "all_earners":
		return rewardsV1.RewardType_REWARD_TYPE_FOR_ALL_EARNERS, nil
	default:
		return -1, fmt.Errorf("unknown reward type '%s'", rewardType)
	}
}

// GetRewardsByAvsForDistributionRoot returns the rewards for a specific distribution root.
//
// TODO(seanmcgary): add pagination if this response gets too large in the future
func (rpc *RpcServer) GetRewardsByAvsForDistributionRoot(ctx context.Context, req *rewardsV1.GetRewardsByAvsForDistributionRootRequest) (*rewardsV1.GetRewardsByAvsForDistributionRootResponse, error) {
	rootIndex := req.GetRootIndex()

	rewards, err := rpc.rewardsDataService.GetRewardsByAvsForDistributionRoot(ctx, rootIndex)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	rewardsResponse := make([]*rewardsV1.AvsReward, 0, len(rewards))
	for _, r := range rewards {
		rewardType, err := convertRewardTypeToEnum(r.RewardType)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		rewardsResponse = append(rewardsResponse, &rewardsV1.AvsReward{
			Earner:     r.Earner,
			Avs:        r.Avs,
			Token:      r.Token,
			Amount:     r.Amount,
			RewardHash: r.RewardHash,
			Snapshot:   r.Snapshot,
			RewardType: rewardType,
		})
	}

	return &rewardsV1.GetRewardsByAvsForDistributionRootResponse{
		Rewards: rewardsResponse,
	}, nil
}

// ListEarnerLifetimeRewards returns the lifetime rewards for an earner, which is a list of:
// - token address
// - total earned
func (s *RpcServer) ListEarnerLifetimeRewards(ctx context.Context, request *rewardsV1.ListEarnerLifetimeRewardsRequest) (*rewardsV1.ListEarnerLifetimeRewardsResponse, error) {
	earnerAddress := request.GetEarnerAddress()
	if earnerAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "earner address is required")
	}

	blockHeight := request.GetBlockHeight()

	// Pagination is not currently supported
	requestedPage := request.GetPagination()
	page := serviceTypes.NewDefaultPagination()
	if requestedPage != nil {
		page.Load(requestedPage.PageNumber, requestedPage.PageSize)
	}

	totalRewards, err := s.rewardsDataService.GetTotalRewardsForEarner(ctx, earnerAddress, nil, blockHeight, false)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &rewardsV1.ListEarnerLifetimeRewardsResponse{
		Rewards: utils.Map(totalRewards, func(r *rewardsDataService.RewardAmount, i uint64) *rewardsV1.RewardAmount {
			return &rewardsV1.RewardAmount{
				Token:  r.Token,
				Amount: r.Amount,
			}
		}),
		// Since we're returning all rewards, there is no next page
		NextPage: nil,
	}, nil
}

func convertHistoricalRewardsToResponse(historicalRewards []*rewardsDataService.HistoricalReward) []*rewardsV1.HistoricalReward {
	// map[token][snapshot] = amount
	tokenMap := make(map[string]map[string]string)
	tokenHistories := make(map[string]*rewardsV1.HistoricalReward)
	tokenResponses := make([]*rewardsV1.HistoricalReward, 0, len(historicalRewards))

	for _, r := range historicalRewards {
		if _, ok := tokenMap[r.Token]; !ok {
			tokenMap[r.Token] = make(map[string]string)
			tokenHistories[r.Token] = &rewardsV1.HistoricalReward{
				Token:   r.Token,
				Amounts: make([]*rewardsV1.HistoricalReward_HistoricalRewardAmount, 0),
			}
		}
		if _, ok := tokenMap[r.Token][r.Snapshot]; !ok {
			tokenMap[r.Token][r.Snapshot] = r.Amount
			tokenHistories[r.Token].Amounts = append(tokenHistories[r.Token].Amounts, &rewardsV1.HistoricalReward_HistoricalRewardAmount{
				Snapshot: r.Snapshot,
				Amount:   r.Amount,
			})
		}
	}
	// ensure the list of snapshots are sorted
	for _, tokenHistory := range tokenHistories {
		slices.SortFunc(tokenHistory.Amounts, func(i, j *rewardsV1.HistoricalReward_HistoricalRewardAmount) int {
			return strings.Compare(i.Snapshot, j.Snapshot)
		})
		tokenResponses = append(tokenResponses, tokenHistory)
	}
	// ensure list of tokens are sorted
	slices.SortFunc(tokenResponses, func(i, j *rewardsV1.HistoricalReward) int {
		return strings.Compare(i.Token, j.Token)
	})
	return tokenResponses
}

// ListEarnerHistoricalRewards returns the historical rewards for an earner for a start/end block range and token list, returning
// a list of:
//   - token address
//     for each token:
//   - amount
//   - snapshot
func (s *RpcServer) ListEarnerHistoricalRewards(ctx context.Context, request *rewardsV1.ListEarnerHistoricalRewardsRequest) (*rewardsV1.ListEarnerHistoricalRewardsResponse, error) {
	earnerAddress := request.GetEarnerAddress()
	if earnerAddress == "" {
		return nil, status.Error(codes.InvalidArgument, "earner address is required")
	}

	startBlock := request.GetStartBlockHeight()
	endBlock := request.GetEndBlockHeight()
	tokens := request.GetTokens()

	historicalRewards, err := s.rewardsDataService.ListHistoricalRewardsForEarner(ctx, earnerAddress, startBlock, endBlock, tokens)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &rewardsV1.ListEarnerHistoricalRewardsResponse{
		Rewards: convertHistoricalRewardsToResponse(historicalRewards),
	}, nil
}
