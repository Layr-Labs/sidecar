package stateManager

import (
	"encoding/binary"
	"errors"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"github.com/ethereum/go-ethereum/common"
	"slices"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/wealdtech/go-merkletree/v2"
	"github.com/wealdtech/go-merkletree/v2/keccak256"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type StateRoot struct {
	EthBlockNumber uint64
	EthBlockHash   string
	StateRoot      string
	CreatedAt      time.Time
}

type EigenStateManager struct {
	StateModels map[int]types.IEigenStateModel
	logger      *zap.Logger
	DB          *gorm.DB
}

func NewEigenStateManager(logger *zap.Logger, grm *gorm.DB) *EigenStateManager {
	return &EigenStateManager{
		StateModels: make(map[int]types.IEigenStateModel),
		logger:      logger,
		DB:          grm,
	}
}

// Allows a model to register itself with the state manager.
func (e *EigenStateManager) RegisterState(model types.IEigenStateModel, index int) {
	if m, ok := e.StateModels[index]; ok {
		e.logger.Sugar().Fatalf("Registering model model at index %d which already exists and belongs to %s", index, m.GetModelName())
	}
	e.StateModels[index] = model
}

// Given a log, allow each state model to determine if/how to process it.
func (e *EigenStateManager) HandleLogStateChange(log *storage.TransactionLog) error {
	e.logger.Sugar().Debugw("Handling log state change", zap.String("transactionHash", log.TransactionHash), zap.Uint64("logIndex", log.LogIndex))
	for _, index := range e.GetSortedModelIndexes() {
		state := e.StateModels[index]
		if state.IsInterestingLog(log) {
			e.logger.Sugar().Debugw("Handling log for model",
				zap.String("model", state.GetModelName()),
				zap.String("transactionHash", log.TransactionHash),
				zap.Uint64("logIndex", log.LogIndex),
				zap.String("eventName", log.EventName),
			)
			_, err := state.HandleStateChange(log)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *EigenStateManager) InitProcessingForBlock(blockNumber uint64) error {
	for _, index := range e.GetSortedModelIndexes() {
		state := e.StateModels[index]
		err := state.SetupStateForBlock(blockNumber)
		if err != nil {
			return err
		}
	}
	return nil
}

// With all transactions/logs processed for a block, commit the final state to the table.
func (e *EigenStateManager) CommitFinalState(blockNumber uint64) error {
	for _, index := range e.GetSortedModelIndexes() {
		state := e.StateModels[index]
		err := state.CommitFinalState(blockNumber)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *EigenStateManager) CleanupProcessedStateForBlock(blockNumber uint64) error {
	for _, index := range e.GetSortedModelIndexes() {
		state := e.StateModels[index]
		err := state.CleanupProcessedStateForBlock(blockNumber)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *EigenStateManager) GenerateStateRoot(blockNumber uint64, blockHash string) (types.StateRoot, error) {
	sortedIndexes := e.GetSortedModelIndexes()
	common.FromHex(blockHash)
	roots := [][]byte{
		types.MerkleLeafPrefix_Block,
		binary.BigEndian.AppendUint64([]byte{}, blockNumber),
		common.FromHex(blockHash),
	}

	for _, state := range sortedIndexes {
		state := e.StateModels[state]
		leaf, err := e.encodeModelLeaf(state, blockNumber)
		if err != nil {
			return "", err
		}

		// a nil value indicates the model did not have any state changes for this block
		if leaf != nil {
			roots = append(roots, leaf)
		}
	}

	tree, err := merkletree.NewTree(
		merkletree.WithData(roots),
		merkletree.WithHashType(keccak256.New()),
	)
	if err != nil {
		return "", err
	}

	return types.StateRoot(utils.ConvertBytesToString(tree.Root())), nil
}

func (e *EigenStateManager) WriteStateRoot(
	blockNumber uint64,
	blockHash string,
	stateroot types.StateRoot,
) (*StateRoot, error) {
	root := &StateRoot{
		EthBlockNumber: blockNumber,
		EthBlockHash:   blockHash,
		StateRoot:      string(stateroot),
	}

	result := e.DB.Model(&StateRoot{}).Clauses(clause.Returning{}).Create(&root)
	if result.Error != nil {
		return nil, result.Error
	}
	return root, nil
}

func (e *EigenStateManager) GetStateRootForBlock(blockNumber uint64) (*StateRoot, error) {
	root := &StateRoot{}
	result := e.DB.Model(&StateRoot{}).Where("eth_block_number = ?", blockNumber).First(&root)
	if result.Error != nil {
		return nil, result.Error
	}
	return root, nil
}

func (e *EigenStateManager) encodeModelLeaf(model types.IEigenStateModel, blockNumber uint64) ([]byte, error) {
	root, err := model.GenerateStateRoot(blockNumber)
	if err != nil {
		return nil, err
	}
	// If there is no root string returned, it means nothing meaningful happened to the model
	// during this block and should not be included in the state root.
	if root == nil {
		return nil, nil
	}
	return append(types.MerkleLeafPrefix_EigenStateRoot, append([]byte(model.GetModelName()), root...)...), nil
}

func (e *EigenStateManager) GetSortedModelIndexes() []int {
	indexes := make([]int, 0, len(e.StateModels))
	for i := range e.StateModels {
		indexes = append(indexes, i)
	}
	slices.Sort(indexes)
	return indexes
}

func (e *EigenStateManager) GetLatestStateRoot() (*StateRoot, error) {
	root := &StateRoot{}
	result := e.DB.Model(&StateRoot{}).Order("eth_block_number desc").First(&root)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return root, nil
		}
		return nil, result.Error
	}
	return root, nil
}

// DeleteCorruptedState deletes state stored that may be incomplete or corrupted
//
// @param startBlock the block number to start deleting state from (inclusive)
// @param endBlock the block number to end deleting state from (inclusive). If 0, delete all state from startBlock.
func (e *EigenStateManager) DeleteCorruptedState(startBlock uint64, endBlock uint64) error {
	for _, index := range e.GetSortedModelIndexes() {
		state := e.StateModels[index]
		err := state.DeleteState(startBlock, endBlock)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *EigenStateManager) GetSubmittedDistributionRoots(blockNumber uint64) ([]*types.SubmittedDistributionRoot, error) {
	roots := make([]*types.SubmittedDistributionRoot, 0)

	res := e.DB.Model(&types.SubmittedDistributionRoot{}).Where("block_number = ?", blockNumber).Find(&roots)
	if res.Error != nil {
		return nil, res.Error
	}
	return roots, nil
}
