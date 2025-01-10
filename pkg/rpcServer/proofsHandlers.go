package rpcServer

import (
	"context"
	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/claimgen"
	sidecarV1 "github.com/Layr-Labs/protocol-apis/gen/protos/eigenlayer/sidecar/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func convertClaimProofToRPCResponse(solidityProof *claimgen.IRewardsCoordinatorRewardsMerkleClaimStrings) *sidecarV1.Proof {
	tokenLeaves := make([]*sidecarV1.TokenLeaf, 0)

	for _, l := range solidityProof.TokenLeaves {
		tokenLeaves = append(tokenLeaves, &sidecarV1.TokenLeaf{
			Token:              l.Token.String(),
			CumulativeEarnings: l.CumulativeEarnings,
		})
	}

	return &sidecarV1.Proof{
		Root:            solidityProof.Root,
		RootIndex:       solidityProof.RootIndex,
		EarnerIndex:     solidityProof.EarnerIndex,
		EarnerTreeProof: solidityProof.EarnerTreeProof,
		EarnerLeaf: &sidecarV1.EarnerLeaf{
			Earner:          solidityProof.EarnerLeaf.Earner.String(),
			EarnerTokenRoot: solidityProof.EarnerLeaf.EarnerTokenRoot,
		},
		LeafIndices:     solidityProof.TokenIndices,
		TokenTreeProofs: solidityProof.TokenTreeProofs,
		TokenLeaves:     tokenLeaves,
	}
}

func (rpc *RpcServer) GenerateClaimProof(ctx context.Context, req *sidecarV1.GenerateClaimProofRequest) (*sidecarV1.GenerateClaimProofResponse, error) {
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

	solidityClaim := claimgen.FormatProofForSolidity(root, claim)

	return &sidecarV1.GenerateClaimProofResponse{
		Proof: convertClaimProofToRPCResponse(solidityClaim),
	}, nil
}
