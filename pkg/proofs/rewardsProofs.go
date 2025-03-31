package proofs

import (
	"fmt"
	rewardsCoordinator "github.com/Layr-Labs/eigenlayer-contracts/pkg/bindings/IRewardsCoordinator"
	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/claimgen"
	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/distribution"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	gethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/wealdtech/go-merkletree/v2"
	"go.uber.org/zap"
)

// RewardsProofsStore manages the generation and caching of Merkle proofs
// for rewards distributions. It provides functionality to generate proofs
// that can be used by earners to claim their rewards.
type RewardsProofsStore struct {
	// rewardsCalculator is used to calculate and merkelize rewards
	rewardsCalculator *rewards.RewardsCalculator
	// logger for logging operations and errors
	logger *zap.Logger
	// rewardsData caches proof data by snapshot date to avoid recalculation
	rewardsData map[string]*ProofData
}

// ProofData contains all the Merkle trees and distribution data needed
// to generate proofs for a specific snapshot date.
type ProofData struct {
	// SnapshotDate is the date of the rewards snapshot
	SnapshotDate string
	// AccountTree is the Merkle tree of all accounts in the distribution
	AccountTree *merkletree.MerkleTree
	// TokenTree maps token addresses to their respective Merkle trees
	TokenTree map[gethcommon.Address]*merkletree.MerkleTree
	// Distribution contains the complete rewards distribution data
	Distribution *distribution.Distribution
}

// NewRewardsProofsStore creates a new RewardsProofsStore instance.
//
// Parameters:
//   - rc: RewardsCalculator for generating rewards data
//   - l: Logger for logging operations
//
// Returns:
//   - *RewardsProofsStore: A new RewardsProofsStore instance
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

// getRewardsDataForSnapshot retrieves or generates proof data for a specific snapshot date.
// If the data is already cached, it returns the cached version; otherwise, it generates
// new proof data and caches it for future use.
//
// Parameters:
//   - snapshot: The date of the rewards snapshot
//
// Returns:
//   - *ProofData: The proof data for the specified snapshot
//   - error: Any error encountered during data retrieval or generation
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

// GenerateRewardsClaimProof generates a Merkle proof for an earner to claim rewards
// for specific tokens from a distribution root.
//
// Parameters:
//   - earnerAddress: The Ethereum address of the earner claiming rewards
//   - tokenAddresses: List of token addresses for which to generate proofs
//   - rootIndex: The index of the distribution root
//
// Returns:
//   - []byte: The Merkle root of the account tree
//   - *rewardsCoordinator.IRewardsCoordinatorRewardsMerkleClaim: The claim data with proofs
//   - error: Any error encountered during proof generation
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
	if distributionRoot == nil {
		return nil, nil, fmt.Errorf("No claimable distribution root found for root index %d", rootIndex)
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
