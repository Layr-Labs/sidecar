package operatorSetOperatorRegistrations

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

type OperatorSetOperatorRegistration struct {
	Operator        string
	Avs             string
	OperatorSetId   uint64
	IsActive        bool
	BlockNumber     uint64
	TransactionHash string
	LogIndex        uint64
}

type OperatorSetOperatorRegistrationModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*OperatorSetOperatorRegistration]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*OperatorSetOperatorRegistration
	committedState   map[uint64][]*OperatorSetOperatorRegistration
}

func NewOperatorSetOperatorRegistrationModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*OperatorSetOperatorRegistrationModel, error) {
	model := &OperatorSetOperatorRegistrationModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*OperatorSetOperatorRegistration),
		committedState:   make(map[uint64][]*OperatorSetOperatorRegistration),
	}

	esm.RegisterState(model, 13)
	return model, nil
}

const OperatorSetOperatorRegistrationModelName = "OperatorSetOperatorRegistrationModel"

func (osor *OperatorSetOperatorRegistrationModel) GetModelName() string {
	return OperatorSetOperatorRegistrationModelName
}

type operatorSetOperatorRegistrationOutputData struct {
	OperatorSet *OperatorSet `json:"operatorSet"`
}

type OperatorSet struct {
	Avs string `json:"avs"`
	Id  uint64 `json:"id"`
}

func parseOperatorSetOperatorRegistrationOutputData(outputDataStr string) (*operatorSetOperatorRegistrationOutputData, error) {
	outputData := &operatorSetOperatorRegistrationOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (osor *OperatorSetOperatorRegistrationModel) handleOperatorSetOperatorRegistrationEvent(log *storage.TransactionLog) (*OperatorSetOperatorRegistration, error) {
	arguments, err := osor.ParseLogArguments(log)
	if err != nil {
		return nil, err
	}

	outputData, err := parseOperatorSetOperatorRegistrationOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}

	isActive := true // default to true for "OperatorAddedToOperatorSet" event
	if log.EventName == "OperatorRemovedFromOperatorSet" {
		isActive = false
	}

	operatorRegistration := &OperatorSetOperatorRegistration{
		Operator:        strings.ToLower(arguments[0].Value.(string)),
		Avs:             strings.ToLower(outputData.OperatorSet.Avs),
		OperatorSetId:   uint64(outputData.OperatorSet.Id),
		IsActive:        isActive,
		BlockNumber:     log.BlockNumber,
		TransactionHash: log.TransactionHash,
		LogIndex:        log.LogIndex,
	}

	osor.logger.Sugar().Infow("Detected operator set operator registration event",
		zap.Uint64("blockNumber", log.BlockNumber),
		zap.String("transactionHash", log.TransactionHash),
		zap.Uint64("logIndex", log.LogIndex),
	)
	return operatorRegistration, nil
}

func (osor *OperatorSetOperatorRegistrationModel) GetStateTransitions() (types.StateTransitions[*OperatorSetOperatorRegistration], []uint64) {
	stateChanges := make(types.StateTransitions[*OperatorSetOperatorRegistration])

	stateChanges[0] = func(log *storage.TransactionLog) (*OperatorSetOperatorRegistration, error) {
		operatorSetOperatorRegistration, err := osor.handleOperatorSetOperatorRegistrationEvent(log)
		if err != nil {
			return nil, err
		}

		slotId := base.NewSlotID(operatorSetOperatorRegistration.TransactionHash, operatorSetOperatorRegistration.LogIndex)

		_, ok := osor.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate operator set operator registration submitted for slot %s at block %d", slotId, log.BlockNumber)
			osor.logger.Sugar().Errorw("Duplicate operator set operator registration submitted", zap.Error(err))
			return nil, err
		}

		osor.stateAccumulator[log.BlockNumber][slotId] = operatorSetOperatorRegistration

		return operatorSetOperatorRegistration, nil
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

func (osor *OperatorSetOperatorRegistrationModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := osor.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.AllocationManager: {
			"OperatorAddedToOperatorSet",
			"OperatorRemovedFromOperatorSet",
		},
	}
}

func (osor *OperatorSetOperatorRegistrationModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := osor.getContractAddressesForEnvironment()
	return osor.BaseEigenState.IsInterestingLog(addresses, log)
}

func (osor *OperatorSetOperatorRegistrationModel) SetupStateForBlock(blockNumber uint64) error {
	osor.stateAccumulator[blockNumber] = make(map[types.SlotID]*OperatorSetOperatorRegistration)
	osor.committedState[blockNumber] = make([]*OperatorSetOperatorRegistration, 0)
	return nil
}

func (osor *OperatorSetOperatorRegistrationModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(osor.stateAccumulator, blockNumber)
	delete(osor.committedState, blockNumber)
	return nil
}

func (osor *OperatorSetOperatorRegistrationModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := osor.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			osor.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (osor *OperatorSetOperatorRegistrationModel) prepareState(blockNumber uint64) ([]*OperatorSetOperatorRegistration, error) {
	accumulatedState, ok := osor.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		osor.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*OperatorSetOperatorRegistration, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (osor *OperatorSetOperatorRegistrationModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	recordsToInsert, err := osor.prepareState(blockNumber)
	if err != nil {
		return err
	}

	insertedRecords, err := base.CommitFinalState(recordsToInsert, ignoreInsertConflicts, osor.GetTableName(), osor.DB)
	if err != nil {
		osor.logger.Sugar().Errorw("Failed to commit final state", zap.Error(err))
		return err
	}
	osor.committedState[blockNumber] = insertedRecords
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (osor *OperatorSetOperatorRegistrationModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := osor.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := osor.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := osor.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		osor.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (osor *OperatorSetOperatorRegistrationModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := osor.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		osor.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (osor *OperatorSetOperatorRegistrationModel) formatMerkleLeafValue(
	operator string,
	avs string,
	operatorSetId uint64,
	isActive bool,
) (string, error) {
	return fmt.Sprintf("%s_%s_%016x_%t", operator, avs, operatorSetId, isActive), nil
}

func (osor *OperatorSetOperatorRegistrationModel) sortValuesForMerkleTree(operatorRegistrations []*OperatorSetOperatorRegistration) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, operatorRegistration := range operatorRegistrations {
		slotID := base.NewSlotID(operatorRegistration.TransactionHash, operatorRegistration.LogIndex)
		value, err := osor.formatMerkleLeafValue(
			operatorRegistration.Operator,
			operatorRegistration.Avs,
			operatorRegistration.OperatorSetId,
			operatorRegistration.IsActive,
		)
		if err != nil {
			osor.logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.String("operator", operatorRegistration.Operator),
				zap.String("avs", operatorRegistration.Avs),
				zap.Uint64("operatorSetId", operatorRegistration.OperatorSetId),
				zap.Bool("isActive", operatorRegistration.IsActive),
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

func (osor *OperatorSetOperatorRegistrationModel) GetTableName() string {
	return "operator_set_operator_registrations"
}

func (osor *OperatorSetOperatorRegistrationModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return osor.BaseEigenState.DeleteState(osor.GetTableName(), startBlockNumber, endBlockNumber, osor.DB)
}

func (osor *OperatorSetOperatorRegistrationModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	records := make([]*OperatorSetOperatorRegistration, 0)
	res := osor.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&records)
	if res.Error != nil {
		osor.logger.Sugar().Errorw("Failed to list records", zap.Error(res.Error))
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (osor *OperatorSetOperatorRegistrationModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	forks, err := osor.globalConfig.GetRewardsSqlForkDates()
	if err != nil {
		osor.logger.Sugar().Errorw("Failed to get rewards sql fork dates", zap.Error(err))
		return false, err
	}

	return blockHeight >= forks[config.RewardsFork_Mississippi].BlockNumber, nil
}
