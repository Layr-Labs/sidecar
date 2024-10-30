package base

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Layr-Labs/go-sidecar/pkg/parser"
	"github.com/Layr-Labs/go-sidecar/pkg/storage"
	"slices"
	"strings"

	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/types"
	"github.com/wealdtech/go-merkletree/v2"
	"github.com/wealdtech/go-merkletree/v2/keccak256"
	orderedmap "github.com/wk8/go-ordered-map/v2"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type BaseEigenState struct {
	Logger *zap.Logger
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

// Include the block number as the first item in the tree.
// This does two things:
// 1. Ensures that the tree is always different for different blocks
// 2. Allows us to have at least 1 value if there are no model changes for a block.
func (b *BaseEigenState) InitializeMerkleTreeBaseStateWithBlock(blockNumber uint64) [][]byte {
	return [][]byte{
		[]byte(fmt.Sprintf("%d", blockNumber)),
	}
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

func (b *BaseEigenState) DeleteState(tableName string, startBlockNumber uint64, endBlockNumber uint64, db *gorm.DB) error {
	if endBlockNumber != 0 && endBlockNumber < startBlockNumber {
		b.Logger.Sugar().Errorw("Invalid block range",
			zap.Uint64("startBlockNumber", startBlockNumber),
			zap.Uint64("endBlockNumber", endBlockNumber),
		)
		return errors.New("Invalid block range; endBlockNumber must be greater than or equal to startBlockNumber")
	}

	// tokenizing the table name apparently doesnt work, so we need to use Sprintf to include it.
	query := fmt.Sprintf(`
		delete from %s
		where block_number >= @startBlockNumber
	`, tableName)
	if endBlockNumber > 0 {
		query += " and block_number <= @endBlockNumber"
	}
	res := db.Exec(query,
		sql.Named("tableName", tableName),
		sql.Named("startBlockNumber", startBlockNumber),
		sql.Named("endBlockNumber", endBlockNumber))
	if res.Error != nil {
		b.Logger.Sugar().Errorw("Failed to delete state", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

type MerkleTreeInput struct {
	SlotID types.SlotID
	Value  []byte
}

// MerkleizeState creates a merkle tree from the given inputs.
//
// Each input includes a SlotID and a byte representation of the state that changed
func (b *BaseEigenState) MerkleizeState(blockNumber uint64, inputs []*MerkleTreeInput) (*merkletree.MerkleTree, error) {
	om := orderedmap.New[types.SlotID, []byte]()

	for _, input := range inputs {
		_, found := om.Get(input.SlotID)
		if !found {
			om.Set(input.SlotID, input.Value)

			prev := om.GetPair(input.SlotID).Prev()
			if prev != nil && prev.Key > input.SlotID {
				om.Delete(input.SlotID)
				return nil, errors.New("slotIDs are not in order")
			}
		} else {
			return nil, fmt.Errorf("duplicate slotID %s", input.SlotID)
		}
	}

	leaves := b.InitializeMerkleTreeBaseStateWithBlock(blockNumber)
	for rootIndex := om.Oldest(); rootIndex != nil; rootIndex = rootIndex.Next() {
		leaves = append(leaves, encodeMerkleLeaf(rootIndex.Key, rootIndex.Value))
	}
	return merkletree.NewTree(
		merkletree.WithData(leaves),
		merkletree.WithHashType(keccak256.New()),
	)
}

func encodeMerkleLeaf(slotID types.SlotID, value []byte) []byte {
	return append([]byte(slotID), value...)
}