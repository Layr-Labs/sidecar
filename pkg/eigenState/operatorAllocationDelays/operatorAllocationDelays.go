package operatorAllocationDelayDelays

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

type OperatorAllocationDelay struct {
	Operator        string
	EffectiveBlock  uint64
	Delay           uint64
	BlockNumber     uint64
	TransactionHash string
	LogIndex        uint64
}

type OperatorAllocationDelayModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*OperatorAllocationDelay]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*OperatorAllocationDelay
	committedState   map[uint64][]*OperatorAllocationDelay
}

func NewOperatorAllocationDelayModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*OperatorAllocationDelayModel, error) {
	model := &OperatorAllocationDelayModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*OperatorAllocationDelay),
		committedState:   make(map[uint64][]*OperatorAllocationDelay),
	}

	esm.RegisterState(model, 21)
	return model, nil
}

const OperatorAllocationDelayModelName = "OperatorAllocationDelayModel"

func (oad *OperatorAllocationDelayModel) GetModelName() string {
	return OperatorAllocationDelayModelName
}

type operatorAllocationDelayOutputData struct {
	Operator    string `json:"operator"`
	EffectBlock uint64 `json:"effectBlock"`
	Delay       uint64 `json:"delay"`
}

func parseOperatorAllocationDelayOutputData(outputDataStr string) (*operatorAllocationDelayOutputData, error) {
	outputData := &operatorAllocationDelayOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (oad *OperatorAllocationDelayModel) handleOperatorAllocationDelayCreatedEvent(log *storage.TransactionLog) (*OperatorAllocationDelay, error) {
	outputData, err := parseOperatorAllocationDelayOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}

	split := &OperatorAllocationDelay{
		Operator:        strings.ToLower(outputData.Operator),
		EffectiveBlock:  outputData.EffectBlock,
		Delay:           outputData.Delay,
		BlockNumber:     log.BlockNumber,
		TransactionHash: log.TransactionHash,
		LogIndex:        log.LogIndex,
	}

	return split, nil
}

func (oad *OperatorAllocationDelayModel) GetStateTransitions() (types.StateTransitions[*OperatorAllocationDelay], []uint64) {
	stateChanges := make(types.StateTransitions[*OperatorAllocationDelay])

	stateChanges[0] = func(log *storage.TransactionLog) (*OperatorAllocationDelay, error) {
		createdEvent, err := oad.handleOperatorAllocationDelayCreatedEvent(log)
		if err != nil {
			return nil, err
		}

		slotId := base.NewSlotID(createdEvent.TransactionHash, createdEvent.LogIndex)

		_, ok := oad.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate operatorAllocationDelay submitted for slot %s at block %d", slotId, log.BlockNumber)
			oad.logger.Sugar().Errorw("Duplicate operatorAllocationDelay submitted", zap.Error(err))
			return nil, err
		}

		oad.stateAccumulator[log.BlockNumber][slotId] = createdEvent

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

func (oad *OperatorAllocationDelayModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := oad.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.AllocationManager: {
			"AllocationDelaySet",
		},
	}
}

func (oad *OperatorAllocationDelayModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := oad.getContractAddressesForEnvironment()
	return oad.BaseEigenState.IsInterestingLog(addresses, log)
}

func (oad *OperatorAllocationDelayModel) SetupStateForBlock(blockNumber uint64) error {
	oad.stateAccumulator[blockNumber] = make(map[types.SlotID]*OperatorAllocationDelay)
	oad.committedState[blockNumber] = make([]*OperatorAllocationDelay, 0)
	return nil
}

func (oad *OperatorAllocationDelayModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(oad.stateAccumulator, blockNumber)
	delete(oad.committedState, blockNumber)
	return nil
}

func (oad *OperatorAllocationDelayModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := oad.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			oad.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (oad *OperatorAllocationDelayModel) prepareState(blockNumber uint64) ([]*OperatorAllocationDelay, error) {
	accumulatedState, ok := oad.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		oad.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*OperatorAllocationDelay, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (oad *OperatorAllocationDelayModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	recordsToInsert, err := oad.prepareState(blockNumber)
	if err != nil {
		return err
	}

	insertedRecords, err := base.CommitFinalState(recordsToInsert, ignoreInsertConflicts, oad.GetTableName(), oad.DB)
	if err != nil {
		oad.logger.Sugar().Errorw("Failed to commit final state", zap.Error(err))
		return err
	}
	oad.committedState[blockNumber] = insertedRecords
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (oad *OperatorAllocationDelayModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := oad.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := oad.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := oad.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		oad.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (oad *OperatorAllocationDelayModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := oad.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		oad.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (oad *OperatorAllocationDelayModel) formatMerkleLeafValue(
	blockNumber uint64,
	operator string,
	effectiveBlock uint64,
	delay uint64,
) (string, error) {
	return fmt.Sprintf("%s_%016x_%016x", operator, effectiveBlock, delay), nil
}

func (oad *OperatorAllocationDelayModel) sortValuesForMerkleTree(records []*OperatorAllocationDelay) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, record := range records {
		slotID := base.NewSlotID(record.TransactionHash, record.LogIndex)
		value, err := oad.formatMerkleLeafValue(record.BlockNumber, record.Operator, record.EffectiveBlock, record.Delay)
		if err != nil {
			oad.logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.Uint64("blockNumber", record.BlockNumber),
				zap.String("operator", record.Operator),
				zap.Uint64("effectiveBlock", record.EffectiveBlock),
				zap.Uint64("delay", record.Delay),
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

func (oad *OperatorAllocationDelayModel) GetTableName() string {
	return "operator_allocation_delays"
}

func (oad *OperatorAllocationDelayModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return oad.BaseEigenState.DeleteState(oad.GetTableName(), startBlockNumber, endBlockNumber, oad.DB)
}

func (oad *OperatorAllocationDelayModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var splits []*OperatorAllocationDelay
	res := oad.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&splits)
	if res.Error != nil {
		oad.logger.Sugar().Errorw("Failed to list records", zap.Error(res.Error))
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(splits), nil
}

func (oad *OperatorAllocationDelayModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	forks, err := oad.globalConfig.GetRewardsSqlForkDates()
	if err != nil {
		oad.logger.Sugar().Errorw("Failed to get rewards sql fork dates", zap.Error(err))
		return false, err
	}

	return blockHeight >= forks[config.RewardsFork_Brazos].BlockNumber, nil
}
