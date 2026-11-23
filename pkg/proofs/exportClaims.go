package proofs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/claimgen"
	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/distribution"
	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/proofDataFetcher/httpProofDataFetcher"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/rewards/rewardsTypes"
	gethcommon "github.com/ethereum/go-ethereum/common"
)

// ClaimFile is one earner's claim against a distribution root, published as claims/<earner>.json.
type ClaimFile struct {
	*claimgen.IRewardsCoordinatorRewardsMerkleClaimStrings
	// Etherscan is the claim as a tuple string, pasteable into Etherscan or the Safe Transaction Builder for processClaim.
	Etherscan string `json:"etherscan"`
}

// RootFile describes the distribution root the claim files were generated for, published as root.json.
type RootFile struct {
	Chain                 string    `json:"chain"`
	RewardsCoordinator    string    `json:"rewardsCoordinator"`
	RootIndex             uint64    `json:"rootIndex"`
	Root                  string    `json:"root"`
	RewardsCalculationEnd time.Time `json:"rewardsCalculationEnd"`
	ActivatedAt           time.Time `json:"activatedAt"`
	Earners               int       `json:"earners"`
	ClaimAmountsSha256    string    `json:"claimAmountsSha256"`
}

// WriteClaimFiles writes everything needed to claim against root without the sidecar:
//   - claim-amounts.json: the distribution as NDJSON, in the format eigenlayer-rewards-proofs reads
//   - claims/<earner>.json: a ready-to-submit claim for every earner, covering all their tokens
//   - root.json: root metadata and the sha256 of claim-amounts.json
//
// The tree is rebuilt from the serialized claim-amounts.json and must reproduce root.Root, so
// nothing is written unless the published data alone regenerates the on-chain root.
func WriteClaimFiles(outDir, chain, rewardsCoordinator string, root *types.SubmittedDistributionRoot, rows []*rewardsTypes.Reward) error {
	var amounts bytes.Buffer
	enc := json.NewEncoder(&amounts)
	for _, r := range rows {
		if err := enc.Encode(distribution.EarnerLine{Earner: r.Earner, Token: r.Token, CumulativeAmount: r.CumulativeAmount}); err != nil {
			return fmt.Errorf("failed to encode claim amount: %w", err)
		}
	}

	data, err := (&httpProofDataFetcher.HttpProofDataFetcher{}).ProcessClaimAmountsFromRawBody(context.Background(), amounts.Bytes())
	if err != nil {
		return fmt.Errorf("failed to merkelize claim amounts: %w", err)
	}
	if !strings.EqualFold(strings.TrimPrefix(data.Hash, "0x"), strings.TrimPrefix(root.Root, "0x")) {
		return fmt.Errorf("computed root %s does not match distribution root %s (index %d)", data.Hash, root.Root, root.RootIndex)
	}

	claimsDir := filepath.Join(outDir, "claims")
	if err := os.MkdirAll(claimsDir, 0o755); err != nil {
		return err
	}

	earners := 0
	for e := data.Distribution.GetStart(); e != nil; e = e.Next() {
		tokens := make([]gethcommon.Address, 0, e.Value.Len())
		for t := e.Value.Oldest(); t != nil; t = t.Next() {
			tokens = append(tokens, t.Key)
		}
		claim, err := claimgen.GetProofForEarner(data.Distribution, uint32(root.RootIndex), data.AccountTree, data.TokenTree, e.Key, tokens)
		if err != nil {
			return fmt.Errorf("failed to generate proof for earner %s: %w", e.Key.Hex(), err)
		}
		solidityClaim := claimgen.FormatProofForSolidity(data.AccountTree.Root(), claim)
		tuple, err := etherscanTuple(solidityClaim)
		if err != nil {
			return err
		}
		if err := writeJson(filepath.Join(claimsDir, strings.ToLower(e.Key.Hex())+".json"), ClaimFile{solidityClaim, tuple}); err != nil {
			return err
		}
		earners++
	}

	if err := os.WriteFile(filepath.Join(outDir, "claim-amounts.json"), amounts.Bytes(), 0o644); err != nil {
		return err
	}
	sum := sha256.Sum256(amounts.Bytes())
	return writeJson(filepath.Join(outDir, "root.json"), RootFile{
		Chain:                 chain,
		RewardsCoordinator:    rewardsCoordinator,
		RootIndex:             root.RootIndex,
		Root:                  data.Hash,
		RewardsCalculationEnd: root.RewardsCalculationEnd.UTC(),
		ActivatedAt:           root.ActivatedAt.UTC(),
		Earners:               earners,
		ClaimAmountsSha256:    hex.EncodeToString(sum[:]),
	})
}

// etherscanTuple renders the RewardsMerkleClaim struct in field order, the input format Etherscan and Safe expect.
func etherscanTuple(c *claimgen.IRewardsCoordinatorRewardsMerkleClaimStrings) (string, error) {
	leaves := make([][]any, len(c.TokenLeaves))
	for i, l := range c.TokenLeaves {
		leaves[i] = []any{l.Token, l.CumulativeEarnings}
	}
	b, err := json.Marshal([]any{
		c.RootIndex,
		c.EarnerIndex,
		c.EarnerTreeProof,
		[]any{c.EarnerLeaf.Earner, c.EarnerLeaf.EarnerTokenRoot},
		c.TokenIndices,
		c.TokenTreeProofs,
		leaves,
	})
	return string(b), err
}

func writeJson(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
