package stateManager

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/eigenState/types"
	"github.com/Layr-Labs/sidecar/internal/storage"
	"github.com/Layr-Labs/sidecar/internal/utils"
	"github.com/wealdtech/go-merkletree/v2"
	"github.com/wealdtech/go-merkletree/v2/keccak256"
	"go.uber.org/zap"
	"slices"
)

type EigenStateManager struct {
	StateModels map[int]types.IEigenStateModel
	logger      *zap.Logger
}

func NewEigenStateManager(logger *zap.Logger) *EigenStateManager {
	return &EigenStateManager{
		StateModels: make(map[int]types.IEigenStateModel),
		logger:      logger,
	}
}

// Allows a model to register itself with the state manager
func (e *EigenStateManager) RegisterState(model types.IEigenStateModel, index int) {
	if m, ok := e.StateModels[index]; ok {
		e.logger.Sugar().Fatalf("Registering model model at index %d which already exists and belongs to %s", index, m.GetModelName())
	}
	e.StateModels[index] = model
}

// Given a log, allow each state model to determine if/how to process it
func (e *EigenStateManager) HandleLogStateChange(log *storage.TransactionLog) error {
	for _, index := range e.GetSortedModelIndexes() {
		state := e.StateModels[index]
		if state.IsInterestingLog(log) {
			_, err := state.HandleStateChange(log)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// With all transactions/logs processed for a block, commit the final state to the table
func (e *EigenStateManager) CommitFinalState(blockNumber uint64) error {
	for _, index := range e.GetSortedModelIndexes() {
		state := e.StateModels[index]
		err := state.WriteFinalState(blockNumber)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *EigenStateManager) GenerateStateRoot(blockNumber uint64) (types.StateRoot, error) {
	sortedIndexes := e.GetSortedModelIndexes()
	roots := [][]byte{
		[]byte(fmt.Sprintf("%d", blockNumber)),
	}

	for _, state := range sortedIndexes {
		state := e.StateModels[state]
		leaf, err := e.encodeModelLeaf(state, blockNumber)
		if err != nil {
			return "", err
		}
		roots = append(roots, leaf)
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

func (e *EigenStateManager) encodeModelLeaf(model types.IEigenStateModel, blockNumber uint64) ([]byte, error) {
	root, err := model.GenerateStateRoot(blockNumber)
	if err != nil {
		return nil, err
	}
	return append([]byte(model.GetModelName()), []byte(root)[:]...), nil
}

func (e *EigenStateManager) GetSortedModelIndexes() []int {
	indexes := make([]int, 0, len(e.StateModels))
	for i := range e.StateModels {
		indexes = append(indexes, i)
	}
	slices.Sort(indexes)
	return indexes
}