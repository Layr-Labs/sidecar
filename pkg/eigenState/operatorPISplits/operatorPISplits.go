package operatorPISplits

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type OperatorPISplit struct {
	Operator               string
	ActivatedAt            *time.Time
	OldOperatorPISplitBips uint64
	NewOperatorPISplitBips uint64
	BlockNumber            uint64
	TransactionHash        string
	LogIndex               uint64
}

type OperatorPISplitModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*OperatorPISplit]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*OperatorPISplit
}

func NewOperatorPISplitModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*OperatorPISplitModel, error) {
	model := &OperatorPISplitModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*OperatorPISplit),
	}

	esm.RegisterState(model, 9)
	return model, nil
}

func (ops *OperatorPISplitModel) GetModelName() string {
	return "OperatorPISplitModel"
}

type operatorPISplitOutputData struct {
	ActivatedAt            uint64 `json:"activatedAt"`
	OldOperatorPISplitBips uint64 `json:"oldOperatorPISplitBips"`
	NewOperatorPISplitBips uint64 `json:"newOperatorPISplitBips"`
}

func parseOperatorPISplitOutputData(outputDataStr string) (*operatorPISplitOutputData, error) {
	outputData := &operatorPISplitOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (ops *OperatorPISplitModel) handleOperatorPISplitBipsSetEvent(log *storage.TransactionLog) (*OperatorPISplit, error) {
	arguments, err := ops.ParseLogArguments(log)
	if err != nil {
		return nil, err
	}

	outputData, err := parseOperatorPISplitOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}

	activatedAt := time.Unix(int64(outputData.ActivatedAt), 0)

	split := &OperatorPISplit{
		Operator:               strings.ToLower(arguments[1].Value.(string)),
		ActivatedAt:            &activatedAt,
		OldOperatorPISplitBips: outputData.OldOperatorPISplitBips,
		NewOperatorPISplitBips: outputData.NewOperatorPISplitBips,
		BlockNumber:            log.BlockNumber,
		TransactionHash:        log.TransactionHash,
		LogIndex:               log.LogIndex,
	}

	return split, nil
}

func (ops *OperatorPISplitModel) GetStateTransitions() (types.StateTransitions[*OperatorPISplit], []uint64) {
	stateChanges := make(types.StateTransitions[*OperatorPISplit])

	stateChanges[0] = func(log *storage.TransactionLog) (*OperatorPISplit, error) {
		operatorPISplit, err := ops.handleOperatorPISplitBipsSetEvent(log)
		if err != nil {
			return nil, err
		}

		slotId := base.NewSlotID(operatorPISplit.TransactionHash, operatorPISplit.LogIndex)

		_, ok := ops.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := xerrors.Errorf("Duplicate operator PI split submitted for slot %s at block %d", slotId, log.BlockNumber)
			ops.logger.Sugar().Errorw("Duplicate operator PI split submitted", zap.Error(err))
			return nil, err
		}

		ops.stateAccumulator[log.BlockNumber][slotId] = operatorPISplit

		return operatorPISplit, nil
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

func (ops *OperatorPISplitModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := ops.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.RewardsCoordinator: {
			"OperatorPISplitBipsSet",
		},
	}
}

func (ops *OperatorPISplitModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := ops.getContractAddressesForEnvironment()
	return ops.BaseEigenState.IsInterestingLog(addresses, log)
}

func (ops *OperatorPISplitModel) SetupStateForBlock(blockNumber uint64) error {
	ops.stateAccumulator[blockNumber] = make(map[types.SlotID]*OperatorPISplit)
	return nil
}

func (ops *OperatorPISplitModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(ops.stateAccumulator, blockNumber)
	return nil
}

func (ops *OperatorPISplitModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := ops.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			ops.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (ops *OperatorPISplitModel) prepareState(blockNumber uint64) ([]*OperatorPISplit, error) {
	accumulatedState, ok := ops.stateAccumulator[blockNumber]
	if !ok {
		err := xerrors.Errorf("No accumulated state found for block %d", blockNumber)
		ops.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*OperatorPISplit, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (ops *OperatorPISplitModel) CommitFinalState(blockNumber uint64) error {
	recordsToInsert, err := ops.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(recordsToInsert) > 0 {
		for _, record := range recordsToInsert {
			res := ops.DB.Model(&OperatorPISplit{}).Clauses(clause.Returning{}).Create(&record)
			if res.Error != nil {
				ops.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
				return res.Error
			}
		}
	}
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (ops *OperatorPISplitModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := ops.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs := ops.sortValuesForMerkleTree(inserts)

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := ops.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		ops.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (ops *OperatorPISplitModel) sortValuesForMerkleTree(splits []*OperatorPISplit) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, split := range splits {
		slotID := base.NewSlotID(split.TransactionHash, split.LogIndex)
		value := fmt.Sprintf("%s_%d_%d_%d", split.Operator, split.ActivatedAt.Unix(), split.OldOperatorPISplitBips, split.NewOperatorPISplitBips)
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

func (ops *OperatorPISplitModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return ops.BaseEigenState.DeleteState("operator_pi_splits", startBlockNumber, endBlockNumber, ops.DB)
}