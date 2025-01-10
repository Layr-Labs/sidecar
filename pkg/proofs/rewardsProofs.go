package proofs

import (
	rewardsCoordinator "github.com/Layr-Labs/eigenlayer-contracts/pkg/bindings/IRewardsCoordinator"
	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/claimgen"
	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/distribution"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	gethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/wealdtech/go-merkletree/v2"
	"go.uber.org/zap"
)

type RewardsProofsStore struct {
	rewardsCalculator *rewards.RewardsCalculator
	logger            *zap.Logger
	rewardsData       map[string]*ProofData
}

type ProofData struct {
	SnapshotDate string
	AccountTree  *merkletree.MerkleTree
	TokenTree    map[gethcommon.Address]*merkletree.MerkleTree
	Distribution *distribution.Distribution
}

func NewRewardsProofsStore(
	rc *rewards.RewardsCalculator,
	l *zap.Logger,
) *RewardsProofsStore {
	return &RewardsProofsStore{
		rewardsCalculator: rc,
		logger:            l,
		rewardsData:       make(map[string]*ProofData),
	}
}

func (rps *RewardsProofsStore) getRewardsDataForSnapshot(snapshot string) (*ProofData, error) {
	data, ok := rps.rewardsData[snapshot]
	if !ok {
		accountTree, tokenTree, distro, err := rps.rewardsCalculator.MerkelizeRewardsForSnapshot(snapshot)
		if err != nil {
			rps.logger.Sugar().Errorw("Failed to fetch rewards for snapshot",
				zap.String("snapshot", snapshot),
				zap.Error(err),
			)
			return nil, err
		}

		data = &ProofData{
			SnapshotDate: snapshot,
			AccountTree:  accountTree,
			TokenTree:    tokenTree,
			Distribution: distro,
		}
		rps.rewardsData[snapshot] = data
	}
	return data, nil
}

func (rps *RewardsProofsStore) GenerateRewardsClaimProof(earnerAddress string, tokenAddresses []string, rootIndex int64) (
	[]byte,
	*rewardsCoordinator.IRewardsCoordinatorRewardsMerkleClaim,
	error,
) {
	distributionRoot, err := rps.rewardsCalculator.FindClaimableDistributionRoot(rootIndex)
	if err != nil {
		rps.logger.Sugar().Errorf("Failed to find claimable distribution root for root_index",
			zap.Int64("rootIndex", rootIndex),
			zap.Error(err),
		)
		return nil, nil, err
	}

	snapshotDate := distributionRoot.GetSnapshotDate()

	// Make sure rewards have been generated for this snapshot.
	// Any snapshot that is >= the provided date is valid since we'll select only data up
	// to the snapshot/cutoff date
	generatedSnapshot, err := rps.rewardsCalculator.GetGeneratedRewardsForSnapshotDate(snapshotDate)
	if err != nil {
		rps.logger.Sugar().Errorf("Failed to get generated rewards for snapshot date", zap.Error(err))
		return nil, nil, err
	}
	rps.logger.Sugar().Infow("Using snapshot for rewards proof",
		zap.String("requestedSnapshot", snapshotDate),
		zap.String("snapshot", generatedSnapshot.SnapshotDate),
	)

	proofData, err := rps.getRewardsDataForSnapshot(snapshotDate)
	if err != nil {
		rps.logger.Sugar().Error("Failed to get rewards data for snapshot",
			zap.String("snapshot", snapshotDate),
			zap.Error(err),
		)
		return nil, nil, err
	}

	tokens := utils.Map(tokenAddresses, func(addr string, i uint64) gethcommon.Address {
		return gethcommon.HexToAddress(addr)
	})
	earner := gethcommon.HexToAddress(earnerAddress)

	claim, err := claimgen.GetProofForEarner(
		proofData.Distribution,
		uint32(distributionRoot.RootIndex),
		proofData.AccountTree,
		proofData.TokenTree,
		earner,
		tokens,
	)
	if err != nil {
		rps.logger.Sugar().Error("Failed to generate claim proof for earner", zap.Error(err))
		return nil, nil, err
	}

	return proofData.AccountTree.Root(), claim, nil
}
