package operatorMaxMagnitudes

import (
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"math/big"
	"slices"
	"sort"
	"strings"
)

type OperatorMaxMagnitude struct {
	Operator        string
	Strategy        string
	MaxMagnitude    string
	BlockNumber     uint64
	TransactionHash string
	LogIndex        uint64
}

type OperatorMaxMagnitudeModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[*OperatorMaxMagnitude]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*OperatorMaxMagnitude
	committedState   map[uint64][]*OperatorMaxMagnitude
}

func NewOperatorMaxMagnitudeModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*OperatorMaxMagnitudeModel, error) {
	model := &OperatorMaxMagnitudeModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*OperatorMaxMagnitude),
		committedState:   make(map[uint64][]*OperatorMaxMagnitude),
	}

	esm.RegisterState(model, 19)
	return model, nil
}

func (ops *OperatorMaxMagnitudeModel) GetModelName() string {
	return "MaxMagnitude"
}

type operatorMaxMagnitudeOutputData struct {
	Operator     string      `json:"operator"`
	Strategy     string      `json:"strategy"`
	MaxMagnitude json.Number `json:"maxMagnitude"`
}

func parseOperatorMaxMagnitudeOutputData(outputDataStr string) (*operatorMaxMagnitudeOutputData, error) {
	outputData := &operatorMaxMagnitudeOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (ops *OperatorMaxMagnitudeModel) handleOperatorMaxMagnitudeCreatedEvent(log *storage.TransactionLog) (*OperatorMaxMagnitude, error) {
	outputData, err := parseOperatorMaxMagnitudeOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}

	operatorMaxMagnitude, success := new(big.Int).SetString(outputData.MaxMagnitude.String(), 10)
	if !success {
		err := fmt.Errorf("Failed to parse operatorMaxMagnitude: %s", outputData.MaxMagnitude.String())
		ops.logger.Sugar().Errorw("Failed to parse operatorMaxMagnitude", zap.Error(err))
		return nil, err
	}

	split := &OperatorMaxMagnitude{
		Operator:        strings.ToLower(outputData.Operator),
		Strategy:        strings.ToLower(outputData.Strategy),
		MaxMagnitude:    operatorMaxMagnitude.String(),
		BlockNumber:     log.BlockNumber,
		TransactionHash: log.TransactionHash,
		LogIndex:        log.LogIndex,
	}

	return split, nil
}

func (ops *OperatorMaxMagnitudeModel) GetStateTransitions() (types.StateTransitions[*OperatorMaxMagnitude], []uint64) {
	stateChanges := make(types.StateTransitions[*OperatorMaxMagnitude])

	stateChanges[0] = func(log *storage.TransactionLog) (*OperatorMaxMagnitude, error) {
		createdEvent, err := ops.handleOperatorMaxMagnitudeCreatedEvent(log)
		if err != nil {
			return nil, err
		}

		slotId := base.NewSlotID(createdEvent.TransactionHash, createdEvent.LogIndex)

		_, ok := ops.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate operatorMaxMagnitude submitted for slot %s at block %d", slotId, log.BlockNumber)
			ops.logger.Sugar().Errorw("Duplicate operator PI split submitted", zap.Error(err))
			return nil, err
		}

		ops.stateAccumulator[log.BlockNumber][slotId] = createdEvent

		return createdEvent, nil
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

func (ops *OperatorMaxMagnitudeModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := ops.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.AllocationManager: {
			"MaxMagnitudeUpdated",
		},
	}
}

func (ops *OperatorMaxMagnitudeModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := ops.getContractAddressesForEnvironment()
	return ops.BaseEigenState.IsInterestingLog(addresses, log)
}

func (ops *OperatorMaxMagnitudeModel) SetupStateForBlock(blockNumber uint64) error {
	ops.stateAccumulator[blockNumber] = make(map[types.SlotID]*OperatorMaxMagnitude)
	ops.committedState[blockNumber] = make([]*OperatorMaxMagnitude, 0)
	return nil
}

func (ops *OperatorMaxMagnitudeModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(ops.stateAccumulator, blockNumber)
	delete(ops.committedState, blockNumber)
	return nil
}

func (ops *OperatorMaxMagnitudeModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
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
func (ops *OperatorMaxMagnitudeModel) prepareState(blockNumber uint64) ([]*OperatorMaxMagnitude, error) {
	accumulatedState, ok := ops.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		ops.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*OperatorMaxMagnitude, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (ops *OperatorMaxMagnitudeModel) CommitFinalState(blockNumber uint64) error {
	recordsToInsert, err := ops.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(recordsToInsert) > 0 {
		for _, record := range recordsToInsert {
			res := ops.DB.Model(&OperatorMaxMagnitude{}).Clauses(clause.Returning{}).Create(&record)
			if res.Error != nil {
				ops.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
				return res.Error
			}
		}
	}
	ops.committedState[blockNumber] = recordsToInsert
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (ops *OperatorMaxMagnitudeModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := ops.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := ops.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

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

func (ops *OperatorMaxMagnitudeModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := ops.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		ops.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (ops *OperatorMaxMagnitudeModel) formatMerkleLeafValue(
	blockNumber uint64,
	operator string,
	strategy string,
	operatorMaxMagnitude string,
) (string, error) {
	return fmt.Sprintf("%s_%s_%s", operator, strategy, operatorMaxMagnitude), nil
}

func (ops *OperatorMaxMagnitudeModel) sortValuesForMerkleTree(records []*OperatorMaxMagnitude) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, record := range records {
		slotID := base.NewSlotID(record.TransactionHash, record.LogIndex)
		value, err := ops.formatMerkleLeafValue(record.BlockNumber, record.Operator, record.Strategy, record.MaxMagnitude)
		if err != nil {
			ops.logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.Uint64("blockNumber", record.BlockNumber),
				zap.String("operator", record.Operator),
				zap.String("strategy", record.Strategy),
				zap.String("operatorMaxMagnitude", record.MaxMagnitude),
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

func (ops *OperatorMaxMagnitudeModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return ops.BaseEigenState.DeleteState("operator_max_magnitudes", startBlockNumber, endBlockNumber, ops.DB)
}

func (ops *OperatorMaxMagnitudeModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var splits []*OperatorMaxMagnitude
	res := ops.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&splits)
	if res.Error != nil {
		ops.logger.Sugar().Errorw("Failed to list records", zap.Error(res.Error))
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(splits), nil
}

func (ops *OperatorMaxMagnitudeModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
