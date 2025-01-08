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
	"gorm.io/gorm/clause"
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
	}

	esm.RegisterState(model, 10)
	return model, nil
}

func (oas *DefaultOperatorSplitModel) GetModelName() string {
	return "DefaultOperatorSplitModel"
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

func (oas *DefaultOperatorSplitModel) handleDefaultOperatorSplitBipsSetEvent(log *storage.TransactionLog) (*DefaultOperatorSplit, error) {
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

func (oas *DefaultOperatorSplitModel) GetStateTransitions() (types.StateTransitions[*DefaultOperatorSplit], []uint64) {
	stateChanges := make(types.StateTransitions[*DefaultOperatorSplit])

	stateChanges[0] = func(log *storage.TransactionLog) (*DefaultOperatorSplit, error) {
		defaultOperatorSplit, err := oas.handleDefaultOperatorSplitBipsSetEvent(log)
		if err != nil {
			return nil, err
		}

		slotId := base.NewSlotID(defaultOperatorSplit.TransactionHash, defaultOperatorSplit.LogIndex)

		_, ok := oas.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate default operator split submitted for slot %s at block %d", slotId, log.BlockNumber)
			oas.logger.Sugar().Errorw("Duplicate default operator split submitted", zap.Error(err))
			return nil, err
		}

		oas.stateAccumulator[log.BlockNumber][slotId] = defaultOperatorSplit

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

func (oas *DefaultOperatorSplitModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := oas.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.RewardsCoordinator: {
			"DefaultOperatorSplitBipsSet",
		},
	}
}

func (oas *DefaultOperatorSplitModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := oas.getContractAddressesForEnvironment()
	return oas.BaseEigenState.IsInterestingLog(addresses, log)
}

func (oas *DefaultOperatorSplitModel) SetupStateForBlock(blockNumber uint64) error {
	oas.stateAccumulator[blockNumber] = make(map[types.SlotID]*DefaultOperatorSplit)
	return nil
}

func (oas *DefaultOperatorSplitModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(oas.stateAccumulator, blockNumber)
	return nil
}

func (oas *DefaultOperatorSplitModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := oas.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			oas.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (oas *DefaultOperatorSplitModel) prepareState(blockNumber uint64) ([]*DefaultOperatorSplit, error) {
	accumulatedState, ok := oas.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		oas.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*DefaultOperatorSplit, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (oas *DefaultOperatorSplitModel) CommitFinalState(blockNumber uint64) error {
	recordsToInsert, err := oas.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(recordsToInsert) > 0 {
		for _, record := range recordsToInsert {
			res := oas.DB.Model(&DefaultOperatorSplit{}).Clauses(clause.Returning{}).Create(&record)
			if res.Error != nil {
				oas.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
				return res.Error
			}
		}
	}
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (oas *DefaultOperatorSplitModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := oas.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs := oas.sortValuesForMerkleTree(inserts)

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := oas.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		oas.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (oas *DefaultOperatorSplitModel) sortValuesForMerkleTree(splits []*DefaultOperatorSplit) []*base.MerkleTreeInput {
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

func (oas *DefaultOperatorSplitModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return oas.BaseEigenState.DeleteState("default_operator_splits", startBlockNumber, endBlockNumber, oas.DB)
}
