package operatorSets

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
	"slices"
	"sort"
	"strings"
)

type OperatorSet struct {
	OperatorSetId   uint64
	Avs             string
	BlockNumber     uint64
	TransactionHash string
	LogIndex        uint64
}

type OperatorSetModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*OperatorSet]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*OperatorSet
	committedState   map[uint64][]*OperatorSet
}

func NewOperatorSetModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*OperatorSetModel, error) {
	model := &OperatorSetModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*OperatorSet),
		committedState:   make(map[uint64][]*OperatorSet),
	}

	esm.RegisterState(model, 15)
	return model, nil
}

func (os *OperatorSetModel) GetModelName() string {
	return "OperatorSetModel"
}

type operatorSetOutputData struct {
	OperatorSet struct {
		Id  uint64 `json:"id"`
		Avs string `json:"avs"`
	} `json:"operatorSet"`
}

func parseOperatorSetOutputData(outputDataStr string) (*operatorSetOutputData, error) {
	outputData := &operatorSetOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (os *OperatorSetModel) handleOperatorSetCreatedEvent(log *storage.TransactionLog) (*OperatorSet, error) {
	outputData, err := parseOperatorSetOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}

	split := &OperatorSet{
		OperatorSetId:   outputData.OperatorSet.Id,
		Avs:             strings.ToLower(outputData.OperatorSet.Avs),
		BlockNumber:     log.BlockNumber,
		TransactionHash: log.TransactionHash,
		LogIndex:        log.LogIndex,
	}

	return split, nil
}

func (os *OperatorSetModel) GetStateTransitions() (types.StateTransitions[*OperatorSet], []uint64) {
	stateChanges := make(types.StateTransitions[*OperatorSet])

	stateChanges[0] = func(log *storage.TransactionLog) (*OperatorSet, error) {
		createdEvent, err := os.handleOperatorSetCreatedEvent(log)
		if err != nil {
			return nil, err
		}

		slotId := base.NewSlotID(createdEvent.TransactionHash, createdEvent.LogIndex)

		_, ok := os.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate operatorSet submitted for slot %s at block %d", slotId, log.BlockNumber)
			os.logger.Sugar().Errorw("Duplicate operatorSet submitted", zap.Error(err))
			return nil, err
		}

		os.stateAccumulator[log.BlockNumber][slotId] = createdEvent

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

func (os *OperatorSetModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := os.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.AllocationManager: {
			"OperatorSetCreated",
		},
	}
}

func (os *OperatorSetModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := os.getContractAddressesForEnvironment()
	return os.BaseEigenState.IsInterestingLog(addresses, log)
}

func (os *OperatorSetModel) SetupStateForBlock(blockNumber uint64) error {
	os.stateAccumulator[blockNumber] = make(map[types.SlotID]*OperatorSet)
	os.committedState[blockNumber] = make([]*OperatorSet, 0)
	return nil
}

func (os *OperatorSetModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(os.stateAccumulator, blockNumber)
	delete(os.committedState, blockNumber)
	return nil
}

func (os *OperatorSetModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := os.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			os.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (os *OperatorSetModel) prepareState(blockNumber uint64) ([]*OperatorSet, error) {
	accumulatedState, ok := os.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		os.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*OperatorSet, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (os *OperatorSetModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	recordsToInsert, err := os.prepareState(blockNumber)
	if err != nil {
		return err
	}

	insertedRecords, err := base.CommitFinalState(recordsToInsert, ignoreInsertConflicts, os.GetTableName(), os.DB)
	if err != nil {
		os.logger.Sugar().Errorw("Failed to commit final state", zap.Error(err))
		return err
	}
	os.committedState[blockNumber] = insertedRecords
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (os *OperatorSetModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := os.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := os.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := os.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		os.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (os *OperatorSetModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := os.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		os.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (os *OperatorSetModel) formatMerkleLeafValue(
	blockNumber uint64,
	operatorSetId uint64,
	avs string,
) (string, error) {
	return fmt.Sprintf("%016x_%s", operatorSetId, avs), nil
}

func (os *OperatorSetModel) sortValuesForMerkleTree(records []*OperatorSet) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, record := range records {
		slotID := base.NewSlotID(record.TransactionHash, record.LogIndex)
		value, err := os.formatMerkleLeafValue(record.BlockNumber, record.OperatorSetId, record.Avs)
		if err != nil {
			os.logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.Uint64("blockNumber", record.BlockNumber),
				zap.Uint64("operatorSetId", record.OperatorSetId),
				zap.String("avs", record.Avs),
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

func (os *OperatorSetModel) GetTableName() string {
	return "operator_sets"
}

func (os *OperatorSetModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return os.BaseEigenState.DeleteState(os.GetTableName(), startBlockNumber, endBlockNumber, os.DB)
}

func (os *OperatorSetModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var splits []*OperatorSet
	res := os.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&splits)
	if res.Error != nil {
		os.logger.Sugar().Errorw("Failed to list records", zap.Error(res.Error))
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(splits), nil
}

func (os *OperatorSetModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
