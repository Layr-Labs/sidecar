package stateManager

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	stateMigratorTypes "github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"github.com/ethereum/go-ethereum/common"
	ddTracer "gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"slices"
	"sync"
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
	StateModels         map[int]types.IEigenStateModel
	PrecommitProcessors map[int]types.IEigenPrecommitProcessor
	logger              *zap.Logger
	DB                  *gorm.DB
	stateMigrator       stateMigratorTypes.IStateMigrator
}

func NewEigenStateManager(sm stateMigratorTypes.IStateMigrator, logger *zap.Logger, grm *gorm.DB) *EigenStateManager {
	return &EigenStateManager{
		StateModels:         make(map[int]types.IEigenStateModel),
		logger:              logger,
		DB:                  grm,
		stateMigrator:       sm,
		PrecommitProcessors: make(map[int]types.IEigenPrecommitProcessor),
	}
}

// Allows a model to register itself with the state manager.
func (e *EigenStateManager) RegisterState(model types.IEigenStateModel, index int) {
	if m, ok := e.StateModels[index]; ok {
		e.logger.Sugar().Fatalf("Registering model model at index %d which already exists and belongs to %s", index, m.GetModelName())
	}
	e.StateModels[index] = model
}

func (e *EigenStateManager) RegisterPrecommitProcessor(precommitProcessor types.IEigenPrecommitProcessor, index int) {
	if m, ok := e.PrecommitProcessors[index]; ok {
		e.logger.Sugar().Fatalf("Registering precommit processor at index %d which already exists and belongs to %s", index, m.GetName())
	}
	e.PrecommitProcessors[index] = precommitProcessor
}

// Given a log, allow each state model to determine if/how to process it.
func (e *EigenStateManager) HandleLogStateChange(ctx context.Context, log *storage.TransactionLog, requireModelActiveForBlock bool) error {
	span, ctx := ddTracer.StartSpanFromContext(ctx, "EigenStateManager::HandleLogStateChange")
	defer span.Finish()

	e.logger.Sugar().Debugw("Handling log state change", zap.String("transactionHash", log.TransactionHash), zap.Uint64("logIndex", log.LogIndex))
	for _, index := range e.GetSortedModelIndexes() {
		state := e.StateModels[index]
		if state.IsInterestingLog(log) {
			isActive, err := state.IsActiveForBlockHeight(log.BlockNumber)
			if err != nil {
				e.logger.Sugar().Errorw("Failed to check if model is active for block",
					zap.String("model", state.GetModelName()),
					zap.Uint64("blockNumber", log.BlockNumber),
					zap.Error(err),
				)
				return err
			}
			if requireModelActiveForBlock && !isActive {
				e.logger.Sugar().Debugw("Model not active for block, skipping",
					zap.String("model", state.GetModelName()),
					zap.Uint64("blockNumber", log.BlockNumber),
				)
				continue
			}
			e.logger.Sugar().Debugw("Handling log for model",
				zap.String("model", state.GetModelName()),
				zap.String("transactionHash", log.TransactionHash),
				zap.Uint64("logIndex", log.LogIndex),
				zap.String("eventName", log.EventName),
			)
			changeSpan, _ := ddTracer.StartSpanFromContext(ctx, "EigenStateManager::HandleLogStateChange::HandleStateChange")
			changeSpan.SetTag("model", state.GetModelName())
			_, err = state.HandleStateChange(log)
			changeSpan.Finish()

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

func (e *EigenStateManager) GetSortedPrecommitProcessorIndexes() []int {
	indexes := make([]int, 0, len(e.PrecommitProcessors))
	for i := range e.PrecommitProcessors {
		indexes = append(indexes, i)
	}
	slices.Sort(indexes)
	return indexes
}

func (e *EigenStateManager) RunPrecommitProcessors(blockNumber uint64) error {
	mappedModels := e.GetModelsMappedByName()
	for _, index := range e.GetSortedPrecommitProcessorIndexes() {
		precommitProcessor := e.PrecommitProcessors[index]
		err := precommitProcessor.Process(blockNumber, mappedModels)
		if err != nil {
			return err
		}
	}
	return nil
}

// With all transactions/logs processed for a block, commit the final state to the table.
func (e *EigenStateManager) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) (map[string][]interface{}, error) {
	committedState := make(map[string][]interface{})
	for _, index := range e.GetSortedModelIndexes() {
		state := e.StateModels[index]
		err := state.CommitFinalState(blockNumber, ignoreInsertConflicts)
		if err != nil {
			return committedState, err
		}
		cs, err := state.GetCommittedState(blockNumber)
		if err != nil {
			return committedState, err
		}
		committedState[state.GetModelName()] = cs
	}
	return committedState, nil
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
	roots := [][]byte{
		append(types.MerkleLeafPrefix_Block, binary.BigEndian.AppendUint64([]byte{}, blockNumber)...),
		append(types.MerkleLeafPrefix_BlockHash, common.FromHex(blockHash)...),
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

	// Handle any migrations needed for the given block number.
	if e.stateMigrator != nil {
		migrationRoot, _, err := e.stateMigrator.RunMigrationsForBlock(blockNumber)
		if err != nil {
			return "", err
		}
		if migrationRoot != nil {
			migrationRoot = append(types.MerkleLeafPrefix_MigrationRoot, migrationRoot...)
			roots = append(roots, migrationRoot)
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

func (e *EigenStateManager) GetModelsMappedByName() map[string]types.IEigenStateModel {
	models := make(map[string]types.IEigenStateModel)
	for _, index := range e.GetSortedModelIndexes() {
		models[e.StateModels[index].GetModelName()] = e.StateModels[index]
	}
	return models
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
	query := `delete from state_roots where eth_block_number >= @startBlock`
	res := e.DB.Exec(query, sql.Named("startBlock", startBlock))
	if res.Error != nil {
		return res.Error
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

type EigenStateResult struct {
	Results []interface{}
	Error   error
}

// ListForBlockRange lists all records for the block range, inclusive of start and end block numbers.
// Each model is processed concurrently in a goroutine
func (e *EigenStateManager) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) (map[string][]interface{}, error) {
	channelMap := make(map[string]chan EigenStateResult)

	var wg sync.WaitGroup
	for _, index := range e.GetSortedModelIndexes() {
		wg.Add(1)
		ch := make(chan EigenStateResult, 1)
		channelMap[e.StateModels[index].GetModelName()] = ch

		go func(ch chan EigenStateResult) {
			defer wg.Done()
			state := e.StateModels[index]
			res, err := state.ListForBlockRange(startBlockNumber, endBlockNumber)

			ch <- EigenStateResult{
				Results: res,
				Error:   err,
			}
		}(ch)
	}
	wg.Wait()
	records := make(map[string][]interface{})
	for name, ch := range channelMap {
		close(ch)
		result := <-ch
		if result.Error != nil {
			return nil, result.Error
		}
		records[name] = result.Results
	}
	return records, nil
}
