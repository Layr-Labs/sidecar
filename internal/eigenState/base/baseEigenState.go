package base

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/Layr-Labs/go-sidecar/internal/eigenState/types"
	"github.com/Layr-Labs/go-sidecar/internal/utils"
	"github.com/wealdtech/go-merkletree/v2"
	"github.com/wealdtech/go-merkletree/v2/keccak256"
	"golang.org/x/xerrors"

	"github.com/Layr-Labs/go-sidecar/internal/parser"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type BaseEigenState struct {
	Logger *zap.Logger
	DB     *gorm.DB
}

func NewBaseEigenState(logger *zap.Logger, db *gorm.DB) BaseEigenState {
	return BaseEigenState{
		Logger: logger,
		DB:     db,
	}
}

func (b *BaseEigenState) ParseLogArguments(log *storage.TransactionLog) ([]parser.Argument, error) {
	arguments := make([]parser.Argument, 0)
	err := json.Unmarshal([]byte(log.Arguments), &arguments)
	if err != nil {
		b.Logger.Sugar().Errorw("Failed to unmarshal arguments",
			zap.Error(err),
			zap.String("transactionHash", log.TransactionHash),
			zap.Uint64("transactionIndex", log.TransactionIndex),
		)
		return nil, err
	}
	return arguments, nil
}

func (b *BaseEigenState) ParseLogOutput(log *storage.TransactionLog) (map[string]interface{}, error) {
	outputData := make(map[string]interface{})
	err := json.Unmarshal([]byte(log.OutputData), &outputData)
	if err != nil {
		b.Logger.Sugar().Errorw("Failed to unmarshal outputData",
			zap.Error(err),
			zap.String("transactionHash", log.TransactionHash),
			zap.Uint64("transactionIndex", log.TransactionIndex),
		)
		return nil, err
	}
	return outputData, nil
}

func (b *BaseEigenState) IsInterestingLog(contractsEvents map[string][]string, log *storage.TransactionLog) bool {
	logAddress := strings.ToLower(log.Address)
	if eventNames, ok := contractsEvents[logAddress]; ok {
		if slices.Contains(eventNames, log.EventName) {
			return true
		}
	}
	return false
}

// HandleLog handles state changes for a given block number.
// it loops through the forkBlockNumbers and calls the state change function for the block number
// this supports models that have different state changes for different block ranges
func (b *BaseEigenState) HandleLog(stateTransitions types.StateTransitions, log *storage.TransactionLog) (interface{}, error) {
	forkBlockNumbers := make([]uint64, 0, len(stateTransitions))
	for blockNumber := range stateTransitions {
		forkBlockNumbers = append(forkBlockNumbers, blockNumber)
	}
	slices.Sort(forkBlockNumbers)
	slices.Reverse(forkBlockNumbers)

	for _, blockNumber := range forkBlockNumbers {
		if log.BlockNumber >= blockNumber {
			b.Logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", blockNumber))

			change, err := stateTransitions[blockNumber](log)
			if err != nil {
				return nil, err
			}

			if change == nil {
				return nil, xerrors.Errorf("No state change found for block %d", blockNumber)
			}
			return change, nil
		}
	}
	return nil, nil
}

func (b *BaseEigenState) DeleteState(tableName string, startBlockNumber uint64, endBlockNumber uint64) error {
	if endBlockNumber != 0 && endBlockNumber < startBlockNumber {
		b.Logger.Sugar().Errorw("Invalid block range",
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
	res := b.DB.Exec(query,
		sql.Named("tableName", tableName),
		sql.Named("startBlockNumber", startBlockNumber),
		sql.Named("endBlockNumber", endBlockNumber))
	if res.Error != nil {
		b.Logger.Sugar().Errorw("Failed to delete state", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

type StateDiff struct {
	SlotID types.SlotID
	Value  []byte
}

// Include the block number as the first item in the tree.
// This does two things:
// 1. Ensures that the tree is always different for different blocks
// 2. Allows us to have at least 1 value if there are no model changes for a block.
func (b *BaseEigenState) InitializeStateTreeForBlock(blockNumber uint64) [][]byte {
	return [][]byte{
		[]byte(fmt.Sprintf("%d", blockNumber)),
	}
}

// MerkleizeState creates a merkle tree from the given diffs.
//
// Each input includes a SlotID and a byte representation of the state that changed
func (b *BaseEigenState) MerkleizeState(blockNumber uint64, stateDiffs []*StateDiff) (types.StateRoot, error) {
	// sort by slotID
	sortedDiffs := make([]*StateDiff, len(stateDiffs))
	copy(sortedDiffs, stateDiffs)
	slices.SortFunc(sortedDiffs, func(a, b *StateDiff) int {
		return strings.Compare(string(a.SlotID), string(b.SlotID))
	})

	// check for duplicates
	for i := 0; i < len(sortedDiffs)-1; i++ {
		if sortedDiffs[i].SlotID == sortedDiffs[i+1].SlotID {
			return "", xerrors.Errorf("Duplicate slotID found: %s", sortedDiffs[i].SlotID)
		}
	}

	leaves := b.InitializeStateTreeForBlock(blockNumber)
	for _, diff := range sortedDiffs {
		leaves = append(leaves, encodeMerkleLeaf(diff.SlotID, diff.Value))
	}

	tree, err := merkletree.NewTree(
		merkletree.WithData(leaves),
		merkletree.WithHashType(keccak256.New()),
	)
	if err != nil {
		return "", err
	}

	return types.StateRoot(utils.ConvertBytesToString(tree.Root())), nil
}

func encodeMerkleLeaf(slotID types.SlotID, value []byte) []byte {
	return append([]byte(slotID), value...)
}
