package operatorSetSplits

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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type OperatorSetSplit struct {
	Operator                string
	Avs                     string
	OperatorSetId           uint64
	ActivatedAt             *time.Time
	OldOperatorSetSplitBips uint64
	NewOperatorSetSplitBips uint64
	BlockNumber             uint64
	TransactionHash         string
	LogIndex                uint64
}

type OperatorSetSplitModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*OperatorSetSplit]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*OperatorSetSplit
	committedState   map[uint64][]*OperatorSetSplit
}

func NewOperatorSetSplitModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*OperatorSetSplitModel, error) {
	model := &OperatorSetSplitModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*OperatorSetSplit),
		committedState:   make(map[uint64][]*OperatorSetSplit),
	}

	esm.RegisterState(model, 12)
	return model, nil
}

func (oss *OperatorSetSplitModel) GetModelName() string {
	return "OperatorSetSplitModel"
}

type operatorSetSplitOutputData struct {
	OperatorSet             *OperatorSet `json:"operatorSet"`
	ActivatedAt             uint64       `json:"activatedAt"`
	OldOperatorSetSplitBips uint64       `json:"oldOperatorSetSplitBips"`
	NewOperatorSetSplitBips uint64       `json:"newOperatorSetSplitBips"`
}

type OperatorSet struct {
	Avs string `json:"avs"`
	Id  uint64 `json:"id"`
}

func parseOperatorSetSplitOutputData(outputDataStr string) (*operatorSetSplitOutputData, error) {
	outputData := &operatorSetSplitOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (oss *OperatorSetSplitModel) handleOperatorSetSplitBipsSetEvent(log *storage.TransactionLog) (*OperatorSetSplit, error) {
	arguments, err := oss.ParseLogArguments(log)
	if err != nil {
		return nil, err
	}

	outputData, err := parseOperatorSetSplitOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}

	activatedAt := time.Unix(int64(outputData.ActivatedAt), 0)

	split := &OperatorSetSplit{
		Operator:                strings.ToLower(arguments[1].Value.(string)),
		Avs:                     strings.ToLower(outputData.OperatorSet.Avs),
		OperatorSetId:           uint64(outputData.OperatorSet.Id),
		ActivatedAt:             &activatedAt,
		OldOperatorSetSplitBips: outputData.OldOperatorSetSplitBips,
		NewOperatorSetSplitBips: outputData.NewOperatorSetSplitBips,
		BlockNumber:             log.BlockNumber,
		TransactionHash:         log.TransactionHash,
		LogIndex:                log.LogIndex,
	}

	return split, nil
}

func (oss *OperatorSetSplitModel) GetStateTransitions() (types.StateTransitions[*OperatorSetSplit], []uint64) {
	stateChanges := make(types.StateTransitions[*OperatorSetSplit])

	stateChanges[0] = func(log *storage.TransactionLog) (*OperatorSetSplit, error) {
		operatorSetSplit, err := oss.handleOperatorSetSplitBipsSetEvent(log)
		if err != nil {
			return nil, err
		}

		slotId := base.NewSlotID(operatorSetSplit.TransactionHash, operatorSetSplit.LogIndex)

		_, ok := oss.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate operator set split submitted for slot %s at block %d", slotId, log.BlockNumber)
			oss.logger.Sugar().Errorw("Duplicate operator set split submitted", zap.Error(err))
			return nil, err
		}

		oss.stateAccumulator[log.BlockNumber][slotId] = operatorSetSplit

		return operatorSetSplit, nil
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

func (oss *OperatorSetSplitModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := oss.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.RewardsCoordinator: {
			"OperatorSetSplitBipsSet",
		},
	}
}

func (oss *OperatorSetSplitModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := oss.getContractAddressesForEnvironment()
	return oss.BaseEigenState.IsInterestingLog(addresses, log)
}

func (oss *OperatorSetSplitModel) SetupStateForBlock(blockNumber uint64) error {
	oss.stateAccumulator[blockNumber] = make(map[types.SlotID]*OperatorSetSplit)
	oss.committedState[blockNumber] = make([]*OperatorSetSplit, 0)
	return nil
}

func (oss *OperatorSetSplitModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(oss.stateAccumulator, blockNumber)
	delete(oss.committedState, blockNumber)
	return nil
}

func (oss *OperatorSetSplitModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := oss.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			oss.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (oss *OperatorSetSplitModel) prepareState(blockNumber uint64) ([]*OperatorSetSplit, error) {
	accumulatedState, ok := oss.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		oss.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*OperatorSetSplit, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (oss *OperatorSetSplitModel) CommitFinalState(blockNumber uint64) error {
	recordsToInsert, err := oss.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(recordsToInsert) > 0 {
		for _, record := range recordsToInsert {
			res := oss.DB.Model(&OperatorSetSplit{}).Clauses(clause.Returning{}).Create(&record)
			if res.Error != nil {
				oss.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
				return res.Error
			}
		}
	}
	oss.committedState[blockNumber] = recordsToInsert
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (oss *OperatorSetSplitModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := oss.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := oss.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := oss.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		oss.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (oss *OperatorSetSplitModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := oss.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		oss.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (oss *OperatorSetSplitModel) formatMerkleLeafValue(
	operator string,
	avs string,
	operatorSetId uint64,
	activatedAt *time.Time,
	oldOperatorSetSplitBips uint64,
	newOperatorSetSplitBips uint64,
) (string, error) {
	return fmt.Sprintf("%s_%s_%016x_%016x_%016x_%016x", operator, avs, operatorSetId, activatedAt.Unix(), oldOperatorSetSplitBips, newOperatorSetSplitBips), nil
}

func (oss *OperatorSetSplitModel) sortValuesForMerkleTree(splits []*OperatorSetSplit) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, split := range splits {
		slotID := base.NewSlotID(split.TransactionHash, split.LogIndex)
		value, err := oss.formatMerkleLeafValue(
			split.Operator,
			split.Avs,
			split.OperatorSetId,
			split.ActivatedAt,
			split.OldOperatorSetSplitBips,
			split.NewOperatorSetSplitBips,
		)
		if err != nil {
			oss.logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.String("operator", split.Operator),
				zap.String("avs", split.Avs),
				zap.Uint64("operatorSetId", split.OperatorSetId),
				zap.Time("activatedAt", *split.ActivatedAt),
				zap.Uint64("oldOperatorSetSplitBips", split.OldOperatorSetSplitBips),
				zap.Uint64("newOperatorSetSplitBips", split.NewOperatorSetSplitBips),
			)
			return nil, err
		}
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: slotID,
			Value:  []byte(value),
		})
	}

	slices.SortFunc(inputs, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})

	return inputs, nil
}

func (oss *OperatorSetSplitModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return oss.BaseEigenState.DeleteState("operator_set_splits", startBlockNumber, endBlockNumber, oss.DB)
}
