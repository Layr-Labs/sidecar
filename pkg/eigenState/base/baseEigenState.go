package base

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"gorm.io/gorm/clause"
	"slices"
	"strings"

	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
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
		append(types.MerkleLeafPrefix_EigenStateBlock, binary.BigEndian.AppendUint64([]byte{}, blockNumber)...),
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

// MerkleizeEigenState creates a merkle tree from the given inputs.
//
// Each input includes a SlotID and a byte representation of the state that changed
func (b *BaseEigenState) MerkleizeEigenState(blockNumber uint64, inputs []*MerkleTreeInput) (*merkletree.MerkleTree, error) {
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
	return append(types.MerkleLeafPrefix_EigenStateChange, append([]byte(slotID), value...)...)
}

func NewSlotID(txHash string, logIndex uint64) types.SlotID {
	return NewSlotIDWithSuffix(txHash, logIndex, "")
}

func NewSlotIDWithSuffix(txHash string, logIndex uint64, suffix string) types.SlotID {
	baseSlotId := fmt.Sprintf("%s_%016x", txHash, logIndex)
	if suffix != "" {
		baseSlotId = fmt.Sprintf("%s_%s", baseSlotId, suffix)
	}
	return types.SlotID(baseSlotId)
}

func CastCommittedStateToInterface[T any](committedState []*T) []interface{} {
	state := make([]interface{}, len(committedState))
	for i, v := range committedState {
		state[i] = v
	}
	return state
}

func CastInterfaceToCommittedState[T any](committedState []interface{}) []*T {
	state := make([]*T, len(committedState))
	for i, v := range committedState {
		state[i] = v.(*T)
	}
	return state
}

func FormatUniqueConstraintName(tableName string) string {
	return fmt.Sprintf("uniq_%s", tableName)
}

func CommitFinalState[T any](
	records []*T,
	ignoreInsertConflicts bool,
	tableName string,
	db *gorm.DB,
) ([]*T, error) {
	if len(records) == 0 {
		return records, nil
	}

	clauses := []clause.Expression{
		clause.Returning{},
	}
	if ignoreInsertConflicts {
		clauses = append(clauses, clause.OnConflict{
			OnConstraint: FormatUniqueConstraintName(tableName),
			DoNothing:    true,
		})
	}

	res := db.Model(&records[0]).Clauses(clauses...).Create(&records)
	return records, res.Error
}
