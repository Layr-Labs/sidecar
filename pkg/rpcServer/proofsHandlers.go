package rpcServer

import (
	"context"

	rewardsCoordinator "github.com/Layr-Labs/eigenlayer-contracts/pkg/bindings/IRewardsCoordinator"
	rewardsV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1/rewards"
	"github.com/Layr-Labs/sidecar/pkg/proofs"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func convertClaimProofToRPCResponse(root []byte, rewardsProof *rewardsCoordinator.IRewardsCoordinatorRewardsMerkleClaim) *rewardsV1.Proof {
	tokenLeaves := make([]*rewardsV1.TokenLeaf, 0)

	for _, l := range rewardsProof.TokenLeaves {
		tokenLeaves = append(tokenLeaves, &rewardsV1.TokenLeaf{
			Token:              l.Token.String(),
			CumulativeEarnings: l.CumulativeEarnings.String(),
		})
	}

	return &rewardsV1.Proof{
		Root:            root,
		RootIndex:       rewardsProof.RootIndex,
		EarnerIndex:     rewardsProof.EarnerIndex,
		EarnerTreeProof: rewardsProof.EarnerTreeProof,
		EarnerLeaf: &rewardsV1.EarnerLeaf{
			Earner:          rewardsProof.EarnerLeaf.Earner.String(),
			EarnerTokenRoot: rewardsProof.EarnerLeaf.EarnerTokenRoot[:],
		},
		TokenIndices:    rewardsProof.TokenIndices,
		TokenTreeProofs: rewardsProof.TokenTreeProofs,
		TokenLeaves:     tokenLeaves,
	}
}

func (rpc *RpcServer) GenerateClaimProof(ctx context.Context, req *rewardsV1.GenerateClaimProofRequest) (*rewardsV1.GenerateClaimProofResponse, error) {
	earner := req.GetEarnerAddress()
	tokens := req.GetTokens()
	rootIndex := req.GetRootIndex()

	var rootIndexVal int64
	if rootIndex == nil {
		rootIndexVal = -1
	} else {
		rootIndexVal = rootIndex.GetValue()
	}

	root, claim, err := rpc.rewardsProofs.GenerateRewardsClaimProof(earner, tokens, rootIndexVal)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to generate claim proof %s", err.Error())
	}
	return &rewardsV1.GenerateClaimProofResponse{
		Proof: convertClaimProofToRPCResponse(root, claim),
	}, nil
}

func (rpc *RpcServer) GenerateClaimProofBulk(ctx context.Context, req *rewardsV1.GenerateClaimProofBulkRequest) (*rewardsV1.GenerateClaimProofBulkResponse, error) {
	earnerToTokens := req.GetEarnerToTokens()
	rootIndex := req.GetRootIndex()

	var rootIndexVal int64
	if rootIndex == nil {
		rootIndexVal = -1
	} else {
		rootIndexVal = rootIndex.GetValue()
	}

	proofsEarnerToTokens := make([]*proofs.EarnerToTokens, len(earnerToTokens))
	for i, et := range earnerToTokens {
		proofsEarnerToTokens[i] = &proofs.EarnerToTokens{
			EarnerAddress: et.GetEarnerAddress(),
			Tokens:        et.GetTokens(),
		}
	}

	root, claims, err := rpc.rewardsProofs.GenerateRewardsClaimProofBulk(proofsEarnerToTokens, rootIndexVal)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to generate claim proof bulk %s", err.Error())
	}
	return &rewardsV1.GenerateClaimProofBulkResponse{
		Proofs: utils.Map(claims, func(c *rewardsCoordinator.IRewardsCoordinatorRewardsMerkleClaim, i uint64) *rewardsV1.Proof {
			return convertClaimProofToRPCResponse(root, c)
		}),
	}, nil
}
