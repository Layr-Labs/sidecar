package defaultOperatorSplits

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type DefaultOperatorSplit struct {
	OldDefaultOperatorSplitBips uint64
	NewDefaultOperatorSplitBips uint64
	BlockNumber                 uint64
	TransactionHash             string
	LogIndex                    uint64
}

type DefaultOperatorSplitModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*DefaultOperatorSplit]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*DefaultOperatorSplit
	committedState   map[uint64][]*DefaultOperatorSplit
}

func NewDefaultOperatorSplitModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*DefaultOperatorSplitModel, error) {
	model := &DefaultOperatorSplitModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*DefaultOperatorSplit),
		committedState:   make(map[uint64][]*DefaultOperatorSplit),
	}

	esm.RegisterState(model, 10)
	return model, nil
}

const DefaultOperatorSplitModelName = "DefaultOperatorSplitModel"

func (dos *DefaultOperatorSplitModel) GetModelName() string {
	return DefaultOperatorSplitModelName
}

type defaultOperatorSplitOutputData struct {
	OldDefaultOperatorSplitBips uint64 `json:"oldDefaultOperatorSplitBips"`
	NewDefaultOperatorSplitBips uint64 `json:"newDefaultOperatorSplitBips"`
}

func parseDefaultOperatorSplitOutputData(outputDataStr string) (*defaultOperatorSplitOutputData, error) {
	outputData := &defaultOperatorSplitOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (dos *DefaultOperatorSplitModel) handleDefaultOperatorSplitBipsSetEvent(log *storage.TransactionLog) (*DefaultOperatorSplit, error) {
	outputData, err := parseDefaultOperatorSplitOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}

	split := &DefaultOperatorSplit{
		OldDefaultOperatorSplitBips: outputData.OldDefaultOperatorSplitBips,
		NewDefaultOperatorSplitBips: outputData.NewDefaultOperatorSplitBips,
		BlockNumber:                 log.BlockNumber,
		TransactionHash:             log.TransactionHash,
		LogIndex:                    log.LogIndex,
	}

	return split, nil
}

func (dos *DefaultOperatorSplitModel) GetStateTransitions() (types.StateTransitions[*DefaultOperatorSplit], []uint64) {
	stateChanges := make(types.StateTransitions[*DefaultOperatorSplit])

	stateChanges[0] = func(log *storage.TransactionLog) (*DefaultOperatorSplit, error) {
		defaultOperatorSplit, err := dos.handleDefaultOperatorSplitBipsSetEvent(log)
		if err != nil {
			return nil, err
		}

		slotId := base.NewSlotID(defaultOperatorSplit.TransactionHash, defaultOperatorSplit.LogIndex)

		_, ok := dos.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate default operator split submitted for slot %s at block %d", slotId, log.BlockNumber)
			dos.logger.Sugar().Errorw("Duplicate default operator split submitted", zap.Error(err))
			return nil, err
		}

		dos.stateAccumulator[log.BlockNumber][slotId] = defaultOperatorSplit

		return defaultOperatorSplit, nil
	}

	// Create an ordered list of block numbers
	blockNumbers := make([]uint64, 0)
	for blockNumber := range stateChanges {
		blockNumbers = append(blockNumbers, blockNumber)
	}
	sort.Slice(blockNumbers, func(i, j int) bool {
		return blockNumbers[i] < blockNumbers[j]
	})
	slices.Reverse(blockNumbers)

	return stateChanges, blockNumbers
}

func (dos *DefaultOperatorSplitModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := dos.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.RewardsCoordinator: {
			"DefaultOperatorSplitBipsSet",
		},
	}
}

func (dos *DefaultOperatorSplitModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := dos.getContractAddressesForEnvironment()
	return dos.BaseEigenState.IsInterestingLog(addresses, log)
}

func (dos *DefaultOperatorSplitModel) SetupStateForBlock(blockNumber uint64) error {
	dos.stateAccumulator[blockNumber] = make(map[types.SlotID]*DefaultOperatorSplit)
	dos.committedState[blockNumber] = make([]*DefaultOperatorSplit, 0)
	return nil
}

func (dos *DefaultOperatorSplitModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(dos.stateAccumulator, blockNumber)
	delete(dos.committedState, blockNumber)
	return nil
}

func (dos *DefaultOperatorSplitModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := dos.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			dos.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

			change, err := stateChanges[blockNumber](log)
			if err != nil {
				return nil, err
			}
			if change == nil {
				return nil, nil
			}
			return change, nil
		}
	}
	return nil, nil
}

// prepareState prepares the state for commit by adding the new state to the existing state.
func (dos *DefaultOperatorSplitModel) prepareState(blockNumber uint64) ([]*DefaultOperatorSplit, error) {
	accumulatedState, ok := dos.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		dos.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*DefaultOperatorSplit, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (dos *DefaultOperatorSplitModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	recordsToInsert, err := dos.prepareState(blockNumber)
	if err != nil {
		return err
	}

	insertedRecords, err := base.CommitFinalState(recordsToInsert, ignoreInsertConflicts, dos.GetTableName(), dos.DB)
	if err != nil {
		dos.logger.Sugar().Errorw("Failed to insert records", zap.Error(err))
		return err
	}
	dos.committedState[blockNumber] = insertedRecords
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (dos *DefaultOperatorSplitModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := dos.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs := dos.sortValuesForMerkleTree(inserts)

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := dos.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		dos.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (dos *DefaultOperatorSplitModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := dos.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		dos.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (dos *DefaultOperatorSplitModel) sortValuesForMerkleTree(splits []*DefaultOperatorSplit) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, split := range splits {
		slotID := base.NewSlotID(split.TransactionHash, split.LogIndex)
		value := fmt.Sprintf("%016x_%016x", split.OldDefaultOperatorSplitBips, split.NewDefaultOperatorSplitBips)
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: slotID,
			Value:  []byte(value),
		})
	}

	slices.SortFunc(inputs, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})

	return inputs
}

func (dos *DefaultOperatorSplitModel) GetTableName() string {
	return "default_operator_splits"
}

func (dos *DefaultOperatorSplitModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return dos.BaseEigenState.DeleteState(dos.GetTableName(), startBlockNumber, endBlockNumber, dos.DB)
}

func (dos *DefaultOperatorSplitModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var splits []*DefaultOperatorSplit
	res := dos.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&splits)
	if res.Error != nil {
		dos.logger.Sugar().Errorw("Failed to list records", zap.Error(res.Error))
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(splits), nil
}

func (dos *DefaultOperatorSplitModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
