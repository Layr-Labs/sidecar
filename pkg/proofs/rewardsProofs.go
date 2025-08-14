package proofs

import (
	"fmt"
	"sync"
	"time"

	rewardsCoordinator "github.com/Layr-Labs/eigenlayer-contracts/pkg/bindings/IRewardsCoordinator"
	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/claimgen"
	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/distribution"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/metrics/metricsTypes"
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
	mu sync.Mutex // Mutex to protect concurrent access to rewardsData
	// rewardsCalculator is used to calculate and merkelize rewards
	rewardsCalculator *rewards.RewardsCalculator
	// logger for logging operations and errors
	logger *zap.Logger
	// rewardsData caches proof data by snapshot date to avoid recalculation
	rewardsData map[string]*ProofData

	metricsSink *metrics.MetricsSink
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

	LastAccessed *time.Time
}

// EarnerToTokens represents an earner address and their associated tokens
// for generating rewards claim proofs. This local struct provides a generic
// interface that doesn't depend on protobuf types.
type EarnerToTokens struct {
	// EarnerAddress is the Ethereum address of the earner
	EarnerAddress string
	// Tokens is a slice of token addresses for this earner
	Tokens []string
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
	ms *metrics.MetricsSink,
	l *zap.Logger,
) *RewardsProofsStore {
	return &RewardsProofsStore{
		rewardsCalculator: rc,
		logger:            l,
		metricsSink:       ms,
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
	rps.mu.Lock()
	defer rps.mu.Unlock()

	now := time.Now()

	data, ok := rps.rewardsData[snapshot]
	if !ok || data == nil {
		_ = rps.metricsSink.Incr(metricsTypes.Metric_Incr_ProofDataCacheMiss, []metricsTypes.MetricsLabel{
			{Name: "snapshot", Value: snapshot},
		}, 1)
		rps.logger.Sugar().Infow("rewards data cache miss",
			zap.String("snapshot", snapshot),
		)
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
			LastAccessed: &now,
		}
		rps.rewardsData[snapshot] = data
	} else {
		rps.logger.Sugar().Infow("rewards data cache hit",
			zap.String("snapshot", snapshot),
		)
		_ = rps.metricsSink.Incr(metricsTypes.Metric_Incr_ProofDataCacheHit, []metricsTypes.MetricsLabel{
			{Name: "snapshot", Value: snapshot},
		}, 1)
	}
	rps.rewardsData[snapshot].LastAccessed = &now

	rps.evictOldRewards()

	return data, nil
}

func (rps *RewardsProofsStore) evictOldRewards() {
	for snapshot, data := range rps.rewardsData {
		if data.LastAccessed == nil {
			continue
		}
		// if the data hasnt been accessed in 3 days, evict it
		if time.Since(*data.LastAccessed) > 24*time.Hour*3 {
			rps.logger.Sugar().Infow("Evicting old rewards data",
				zap.String("snapshot", snapshot),
				zap.Time("lastAccessed", *data.LastAccessed),
			)
			delete(rps.rewardsData, snapshot)
			_ = rps.metricsSink.Incr(metricsTypes.Metric_Incr_ProofDataCacheEvicted, []metricsTypes.MetricsLabel{
				{Name: "snapshot", Value: snapshot},
			}, 1)
		}
	}
}

func (rps *RewardsProofsStore) generateProofDataForRootIndex(rootIndex int64) (*ProofData, *types.SubmittedDistributionRoot, error) {
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

	return proofData, distributionRoot, nil
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
	proofData, distributionRoot, err := rps.generateProofDataForRootIndex(rootIndex)
	if err != nil {
		rps.logger.Sugar().Error("Failed to generate proof data for root index", zap.Error(err))
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

// GenerateRewardsClaimProofBulk generates Merkle proofs for multiple earner-token combinations
// for claiming rewards against the RewardsCoordinator contract.
//
// This method processes multiple earner addresses with their associated tokens in a single operation,
// improving efficiency when generating proofs for batch claims.
//
// Parameters:
//   - earnerTokens: A slice of EarnerTokens, each containing an earner address and their tokens
//   - rootIndex: The distribution root index to use for proof generation. If -1, uses the active root
//
// Returns:
//   - []byte: The Merkle root of the account tree (same for all proofs)
//   - []*rewardsCoordinator.IRewardsCoordinatorRewardsMerkleClaim: Slice of claim data with proofs
//   - error: Any error encountered during proof generation
func (rps *RewardsProofsStore) GenerateRewardsClaimProofBulk(earnerToTokens []*EarnerToTokens, rootIndex int64) (
	[]byte,
	[]*rewardsCoordinator.IRewardsCoordinatorRewardsMerkleClaim,
	error,
) {
	proofData, distributionRoot, err := rps.generateProofDataForRootIndex(rootIndex)
	if err != nil {
		rps.logger.Sugar().Error("Failed to generate proof data for root index", zap.Error(err))
		return nil, nil, err
	}

	// Generate proofs for each earner-token combination
	claims := make([]*rewardsCoordinator.IRewardsCoordinatorRewardsMerkleClaim, len(earnerToTokens))
	for i, earnerToken := range earnerToTokens {
		// Convert token addresses to common.Address
		tokens := utils.Map(earnerToken.Tokens, func(addr string, i uint64) gethcommon.Address {
			return gethcommon.HexToAddress(addr)
		})
		earner := gethcommon.HexToAddress(earnerToken.EarnerAddress)

		// Generate proof for this earner
		claim, err := claimgen.GetProofForEarner(
			proofData.Distribution,
			uint32(distributionRoot.RootIndex),
			proofData.AccountTree,
			proofData.TokenTree,
			earner,
			tokens,
		)
		if err != nil {
			rps.logger.Sugar().Error("Failed to generate claim proof for earner",
				zap.String("earnerAddress", earnerToken.EarnerAddress),
				zap.Strings("tokens", earnerToken.Tokens),
				zap.Error(err),
			)
			return nil, nil, fmt.Errorf("failed to generate proof for earner %s: %w", earnerToken.EarnerAddress, err)
		}
		claims[i] = claim
	}

	return proofData.AccountTree.Root(), claims, nil
}
