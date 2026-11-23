package proofs

import (
	"bytes"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Layr-Labs/eigenlayer-rewards-proofs/pkg/distribution"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/rewards/rewardsTypes"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/stretchr/testify/require"
)

// verifyInclusion mirrors Merkle.verifyInclusionKeccak in the RewardsCoordinator contracts.
func verifyInclusion(t *testing.T, proofHex string, root []byte, leaf []byte, index uint32) {
	proof := hexutil.MustDecode(proofHex)
	require.Equal(t, 0, len(proof)%32)
	h := crypto.Keccak256(leaf)
	for i := 0; i < len(proof); i += 32 {
		if index%2 == 0 {
			h = crypto.Keccak256(h, proof[i:i+32])
		} else {
			h = crypto.Keccak256(proof[i:i+32], h)
		}
		index /= 2
	}
	require.True(t, bytes.Equal(h, root), "proof does not verify against root")
}

func Test_WriteClaimFiles(t *testing.T) {
	earners := []string{
		"0x1111111111111111111111111111111111111111",
		"0x2222222222222222222222222222222222222222",
		"0x3333333333333333333333333333333333333333",
	}
	tokens := []string{
		"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	rows := make([]*rewardsTypes.Reward, 0)
	lines := make([]*distribution.EarnerLine, 0)
	for i, e := range earners {
		for j, tok := range tokens {
			amount := big.NewInt(int64(1000*(i+1) + j)).String()
			rows = append(rows, &rewardsTypes.Reward{Earner: e, Token: tok, CumulativeAmount: amount})
			lines = append(lines, &distribution.EarnerLine{Earner: e, Token: tok, CumulativeAmount: amount})
		}
	}

	// the root as the sidecar computes it when validating posted roots
	distro := distribution.NewDistribution()
	require.NoError(t, distro.LoadLines(lines))
	accountTree, _, err := distro.Merklize()
	require.NoError(t, err)
	root := &types.SubmittedDistributionRoot{Root: utils.ConvertBytesToString(accountTree.Root()), RootIndex: 7}

	t.Run("writes a verifiable claim for every earner", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, WriteClaimFiles(dir, "mainnet", "0x7750d328b314effa365a0402ccfd489b80b0adda", root, rows))

		for _, e := range earners {
			raw, err := os.ReadFile(filepath.Join(dir, "claims", e+".json"))
			require.NoError(t, err)
			var c ClaimFile
			require.NoError(t, json.Unmarshal(raw, &c))

			require.Equal(t, root.Root, c.Root)
			require.Equal(t, uint32(7), c.RootIndex)
			require.Len(t, c.TokenLeaves, len(tokens))

			tokenRoot := hexutil.MustDecode(c.EarnerLeaf.EarnerTokenRoot)
			verifyInclusion(t, c.EarnerTreeProof, accountTree.Root(), distribution.EncodeAccountLeaf(c.EarnerLeaf.Earner, tokenRoot), c.EarnerIndex)
			for i, leaf := range c.TokenLeaves {
				amount, ok := new(big.Int).SetString(leaf.CumulativeEarnings, 10)
				require.True(t, ok)
				verifyInclusion(t, c.TokenTreeProofs[i], tokenRoot, distribution.EncodeTokenLeaf(leaf.Token, amount), c.TokenIndices[i])
			}

			var tuple []json.RawMessage
			require.NoError(t, json.Unmarshal([]byte(c.Etherscan), &tuple))
			require.Len(t, tuple, 7)
			var tupleLeaves [][]string
			require.NoError(t, json.Unmarshal(tuple[6], &tupleLeaves))
			require.Len(t, tupleLeaves, len(tokens))
		}

		var rf RootFile
		raw, err := os.ReadFile(filepath.Join(dir, "root.json"))
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(raw, &rf))
		require.Equal(t, 3, rf.Earners)
		require.Equal(t, root.Root, rf.Root)
		require.Len(t, rf.ClaimAmountsSha256, 64)
	})

	t.Run("refuses to write anything when the root does not match", func(t *testing.T) {
		dir := t.TempDir()
		bogus := *root
		bogus.Root = "0x" + strings.Repeat("00", 32)
		require.Error(t, WriteClaimFiles(dir, "mainnet", "0x7750d328b314effa365a0402ccfd489b80b0adda", &bogus, rows))

		entries, err := os.ReadDir(dir)
		require.NoError(t, err)
		require.Empty(t, entries)
	})
}
