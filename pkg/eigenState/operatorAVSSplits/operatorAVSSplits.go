package operatorAVSSplits

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

type OperatorAVSSplit struct {
	Operator                string
	Avs                     string
	ActivatedAt             *time.Time
	OldOperatorAVSSplitBips uint64
	NewOperatorAVSSplitBips uint64
	BlockNumber             uint64
	TransactionHash         string
	LogIndex                uint64
}

type OperatorAVSSplitModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*OperatorAVSSplit]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*OperatorAVSSplit
	committedState   map[uint64][]*OperatorAVSSplit
}

func NewOperatorAVSSplitModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*OperatorAVSSplitModel, error) {
	model := &OperatorAVSSplitModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*OperatorAVSSplit),
		committedState:   make(map[uint64][]*OperatorAVSSplit),
	}

	esm.RegisterState(model, 8)
	return model, nil
}

func (oas *OperatorAVSSplitModel) GetModelName() string {
	return "OperatorAVSSplitModel"
}

type operatorAVSSplitOutputData struct {
	ActivatedAt             uint64 `json:"activatedAt"`
	OldOperatorAVSSplitBips uint64 `json:"oldOperatorAVSSplitBips"`
	NewOperatorAVSSplitBips uint64 `json:"newOperatorAVSSplitBips"`
}

func parseOperatorAVSSplitOutputData(outputDataStr string) (*operatorAVSSplitOutputData, error) {
	outputData := &operatorAVSSplitOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (oas *OperatorAVSSplitModel) handleOperatorAVSSplitBipsSetEvent(log *storage.TransactionLog) (*OperatorAVSSplit, error) {
	arguments, err := oas.ParseLogArguments(log)
	if err != nil {
		return nil, err
	}

	outputData, err := parseOperatorAVSSplitOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}

	activatedAt := time.Unix(int64(outputData.ActivatedAt), 0)

	split := &OperatorAVSSplit{
		Operator:                strings.ToLower(arguments[1].Value.(string)),
		Avs:                     strings.ToLower(arguments[2].Value.(string)),
		ActivatedAt:             &activatedAt,
		OldOperatorAVSSplitBips: outputData.OldOperatorAVSSplitBips,
		NewOperatorAVSSplitBips: outputData.NewOperatorAVSSplitBips,
		BlockNumber:             log.BlockNumber,
		TransactionHash:         log.TransactionHash,
		LogIndex:                log.LogIndex,
	}

	return split, nil
}

func (oas *OperatorAVSSplitModel) GetStateTransitions() (types.StateTransitions[*OperatorAVSSplit], []uint64) {
	stateChanges := make(types.StateTransitions[*OperatorAVSSplit])

	stateChanges[0] = func(log *storage.TransactionLog) (*OperatorAVSSplit, error) {
		operatorAVSSplit, err := oas.handleOperatorAVSSplitBipsSetEvent(log)
		if err != nil {
			return nil, err
		}

		slotId := base.NewSlotID(operatorAVSSplit.TransactionHash, operatorAVSSplit.LogIndex)

		_, ok := oas.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate operator AVS split submitted for slot %s at block %d", slotId, log.BlockNumber)
			oas.logger.Sugar().Errorw("Duplicate operator AVS split submitted", zap.Error(err))
			return nil, err
		}

		oas.stateAccumulator[log.BlockNumber][slotId] = operatorAVSSplit

		return operatorAVSSplit, nil
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

func (oas *OperatorAVSSplitModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := oas.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.RewardsCoordinator: {
			"OperatorAVSSplitBipsSet",
		},
	}
}

func (oas *OperatorAVSSplitModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := oas.getContractAddressesForEnvironment()
	return oas.BaseEigenState.IsInterestingLog(addresses, log)
}

func (oas *OperatorAVSSplitModel) SetupStateForBlock(blockNumber uint64) error {
	oas.stateAccumulator[blockNumber] = make(map[types.SlotID]*OperatorAVSSplit)
	oas.committedState[blockNumber] = make([]*OperatorAVSSplit, 0)
	return nil
}

func (oas *OperatorAVSSplitModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(oas.stateAccumulator, blockNumber)
	delete(oas.committedState, blockNumber)
	return nil
}

func (oas *OperatorAVSSplitModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
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
func (oas *OperatorAVSSplitModel) prepareState(blockNumber uint64) ([]*OperatorAVSSplit, error) {
	accumulatedState, ok := oas.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		oas.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*OperatorAVSSplit, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (oas *OperatorAVSSplitModel) CommitFinalState(blockNumber uint64) error {
	recordsToInsert, err := oas.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(recordsToInsert) > 0 {
		for _, record := range recordsToInsert {
			res := oas.DB.Model(&OperatorAVSSplit{}).Clauses(clause.Returning{}).Create(&record)
			if res.Error != nil {
				oas.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
				return res.Error
			}
		}
	}
	oas.committedState[blockNumber] = recordsToInsert
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (oas *OperatorAVSSplitModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := oas.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := oas.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

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

func (oas *OperatorAVSSplitModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := oas.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		oas.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (oas *OperatorAVSSplitModel) formatMerkleLeafValue(
	blockNumber uint64,
	operator string,
	avs string,
	activatedAt *time.Time,
	oldOperatorAVSSplitBips uint64,
	newOperatorAVSSplitBips uint64,
) (string, error) {
	modelForks, err := oas.globalConfig.GetModelForks()
	if err != nil {
		return "", err
	}
	if oas.globalConfig.ChainIsOneOf(config.Chain_Holesky, config.Chain_Preprod) && blockNumber < modelForks[config.ModelFork_Austin] {
		// This format was used on preprod and testnet for rewards-v2 before launching to mainnet
		return fmt.Sprintf("%s_%s_%d_%d_%d", operator, avs, activatedAt.Unix(), oldOperatorAVSSplitBips, newOperatorAVSSplitBips), nil
	}

	return fmt.Sprintf("%s_%s_%016x_%016x_%016x", operator, avs, activatedAt.Unix(), oldOperatorAVSSplitBips, newOperatorAVSSplitBips), nil
}

func (oas *OperatorAVSSplitModel) sortValuesForMerkleTree(splits []*OperatorAVSSplit) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, split := range splits {
		slotID := base.NewSlotID(split.TransactionHash, split.LogIndex)
		value, err := oas.formatMerkleLeafValue(split.BlockNumber, split.Operator, split.Avs, split.ActivatedAt, split.OldOperatorAVSSplitBips, split.NewOperatorAVSSplitBips)
		if err != nil {
			oas.logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.Uint64("blockNumber", split.BlockNumber),
				zap.String("operator", split.Operator),
				zap.String("avs", split.Avs),
				zap.Time("activatedAt", *split.ActivatedAt),
				zap.Uint64("oldOperatorAVSSplitBips", split.OldOperatorAVSSplitBips),
				zap.Uint64("newOperatorAVSSplitBips", split.NewOperatorAVSSplitBips),
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

func (oas *OperatorAVSSplitModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return oas.BaseEigenState.DeleteState("operator_avs_splits", startBlockNumber, endBlockNumber, oas.DB)
}
