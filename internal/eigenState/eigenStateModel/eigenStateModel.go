package eigenStateModel

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/Layr-Labs/go-sidecar/internal/eigenState/types"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/utils"
	"github.com/wealdtech/go-merkletree/v2"
	"github.com/wealdtech/go-merkletree/v2/keccak256"

	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"go.uber.org/zap"
)

type EigenStateModel struct {
	types.IBaseEigenStateModel
}

var _ types.IEigenStateModel = (*EigenStateModel)(nil)

func NewEigenStateModel(base types.IBaseEigenStateModel) *EigenStateModel {
	return &EigenStateModel{
		IBaseEigenStateModel: base,
	}
}

func (m *EigenStateModel) IsInterestingLog(log *storage.TransactionLog) bool {
	interestingLogMap := m.GetInterestingLogMap()
	logAddress := strings.ToLower(log.Address)
	if eventNames, ok := interestingLogMap[logAddress]; ok {
		if slices.Contains(eventNames, log.EventName) {
			return true
		}
	}
	return false
}

func (m *EigenStateModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	tableName := m.TableName()
	if endBlockNumber != 0 && endBlockNumber < startBlockNumber {
		m.Logger().Sugar().Errorw("Invalid block range",
			zap.Uint64("startBlockNumber", startBlockNumber),
			zap.Uint64("endBlockNumber", endBlockNumber),
		)
		return errors.New("invalid block range; endBlockNumber must be greater than or equal to startBlockNumber")
	}

	// tokenizing the table name apparently doesnt work, so we need to use Sprintf to include it.
	query := fmt.Sprintf(`
		delete from %s
		where block_number >= @startBlockNumber
	`, tableName)
	if endBlockNumber > 0 {
		query += " and block_number <= @endBlockNumber"
	}
	res := m.DB().Exec(query,
		sql.Named("tableName", tableName),
		sql.Named("startBlockNumber", startBlockNumber),
		sql.Named("endBlockNumber", endBlockNumber))
	if res.Error != nil {
		m.Logger().Sugar().Errorw("Failed to delete state", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

type MerkleTreeInput struct {
	SlotID types.SlotID
	Value  []byte
}

// Include the block number as the first item in the tree.
// This does two things:
// 1. Ensures that the tree is always different for different blocks
// 2. Allows us to have at least 1 value if there are no model changes for a block.
func (m *EigenStateModel) InitializeLeavesForBlock(blockNumber uint64) [][]byte {
	return [][]byte{
		[]byte(fmt.Sprintf("%d", blockNumber)),
	}
}

// MerkleizeState creates a merkle tree from the given inputs.
//
// Each input includes a SlotID and a byte representation of the state that changed
func (m *EigenStateModel) GenerateStateRoot(blockNumber uint64) (types.StateRoot, error) {
	diffs, err := m.GetStateDiffs(blockNumber)
	if err != nil {
		return "", err
	}

	// sort the inputs by their slotID
	sort.Slice(diffs, func(i, j int) bool {
		return diffs[i].SlotID < diffs[j].SlotID
	})

	// make sure there are no duplicate slotIDs
	for i := 1; i < len(diffs); i++ {
		if diffs[i].SlotID == diffs[i-1].SlotID {
			return "", fmt.Errorf("duplicate slotID %s", diffs[i].SlotID)
		}
	}

	leaves := m.InitializeLeavesForBlock(blockNumber)
	for _, diff := range diffs {
		leaves = append(leaves, encodeMerkleLeaf(diff.SlotID, diff.Value))
	}
	tree, err := merkletree.NewTree(
		merkletree.WithData(leaves),
		merkletree.WithHashType(keccak256.New()),
	)
	if err != nil {
		return "", nil
	}

	return types.StateRoot(utils.ConvertBytesToString(tree.Root())), nil
}

func encodeMerkleLeaf(slotID types.SlotID, value []byte) []byte {
	return append([]byte(slotID), value...)
}
