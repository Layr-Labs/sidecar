package operatorSetStrategyRegistrations

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

type OperatorSetStrategyRegistration struct {
	Strategy        string
	Avs             string
	OperatorSetId   uint64
	IsActive        bool
	BlockNumber     uint64
	TransactionHash string
	LogIndex        uint64
}

type OperatorSetStrategyRegistrationModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*OperatorSetStrategyRegistration]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*OperatorSetStrategyRegistration
	committedState   map[uint64][]*OperatorSetStrategyRegistration
}

func NewOperatorSetStrategyRegistrationModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*OperatorSetStrategyRegistrationModel, error) {
	model := &OperatorSetStrategyRegistrationModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*OperatorSetStrategyRegistration),
		committedState:   make(map[uint64][]*OperatorSetStrategyRegistration),
	}

	esm.RegisterState(model, 14)
	return model, nil
}

func (ossr *OperatorSetStrategyRegistrationModel) GetModelName() string {
	return "OperatorSetStrategyRegistrationModel"
}

type operatorSetStrategyRegistrationOutputData struct {
	OperatorSet *OperatorSet `json:"operatorSet"`
	Strategy    string       `json:"strategy"`
}

type OperatorSet struct {
	Avs string `json:"avs"`
	Id  uint64 `json:"id"`
}

func parseOperatorSetStrategyRegistrationOutputData(outputDataStr string) (*operatorSetStrategyRegistrationOutputData, error) {
	outputData := &operatorSetStrategyRegistrationOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (ossr *OperatorSetStrategyRegistrationModel) handleOperatorSetStrategyRegistrationEvent(log *storage.TransactionLog) (*OperatorSetStrategyRegistration, error) {
	outputData, err := parseOperatorSetStrategyRegistrationOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}

	isActive := true // default to true for "StrategyAddedToOperatorSet" event
	if log.EventName == "StrategyRemovedFromOperatorSet" {
		isActive = false
	}

	strategyRegistration := &OperatorSetStrategyRegistration{
		Strategy:        outputData.Strategy,
		Avs:             outputData.OperatorSet.Avs,
		OperatorSetId:   outputData.OperatorSet.Id,
		IsActive:        isActive,
		BlockNumber:     log.BlockNumber,
		TransactionHash: log.TransactionHash,
		LogIndex:        log.LogIndex,
	}

	return strategyRegistration, nil
}

func (ossr *OperatorSetStrategyRegistrationModel) GetStateTransitions() (types.StateTransitions[*OperatorSetStrategyRegistration], []uint64) {
	stateChanges := make(types.StateTransitions[*OperatorSetStrategyRegistration])

	stateChanges[0] = func(log *storage.TransactionLog) (*OperatorSetStrategyRegistration, error) {
		operatorSetStrategyRegistration, err := ossr.handleOperatorSetStrategyRegistrationEvent(log)
		if err != nil {
			return nil, err
		}

		slotId := base.NewSlotID(operatorSetStrategyRegistration.TransactionHash, operatorSetStrategyRegistration.LogIndex)

		_, ok := ossr.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate operator set strategy registration submitted for slot %s at block %d", slotId, log.BlockNumber)
			ossr.logger.Sugar().Errorw("Duplicate operator set strategy registration submitted", zap.Error(err))
			return nil, err
		}

		ossr.stateAccumulator[log.BlockNumber][slotId] = operatorSetStrategyRegistration

		return operatorSetStrategyRegistration, nil
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

func (ossr *OperatorSetStrategyRegistrationModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := ossr.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.AllocationManager: {
			"StrategyAddedToOperatorSet",
			"StrategyRemovedFromOperatorSet",
		},
	}
}

func (ossr *OperatorSetStrategyRegistrationModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := ossr.getContractAddressesForEnvironment()
	return ossr.BaseEigenState.IsInterestingLog(addresses, log)
}

func (ossr *OperatorSetStrategyRegistrationModel) SetupStateForBlock(blockNumber uint64) error {
	ossr.stateAccumulator[blockNumber] = make(map[types.SlotID]*OperatorSetStrategyRegistration)
	ossr.committedState[blockNumber] = make([]*OperatorSetStrategyRegistration, 0)
	return nil
}

func (ossr *OperatorSetStrategyRegistrationModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(ossr.stateAccumulator, blockNumber)
	delete(ossr.committedState, blockNumber)
	return nil
}

func (ossr *OperatorSetStrategyRegistrationModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := ossr.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			ossr.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (ossr *OperatorSetStrategyRegistrationModel) prepareState(blockNumber uint64) ([]*OperatorSetStrategyRegistration, error) {
	accumulatedState, ok := ossr.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		ossr.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*OperatorSetStrategyRegistration, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (ossr *OperatorSetStrategyRegistrationModel) CommitFinalState(blockNumber uint64) error {
	recordsToInsert, err := ossr.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(recordsToInsert) > 0 {
		for _, record := range recordsToInsert {
			res := ossr.DB.Model(&OperatorSetStrategyRegistration{}).Clauses(clause.Returning{}).Create(&record)
			if res.Error != nil {
				ossr.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
				return res.Error
			}
		}
	}
	ossr.committedState[blockNumber] = recordsToInsert
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (ossr *OperatorSetStrategyRegistrationModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := ossr.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := ossr.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := ossr.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		ossr.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (ossr *OperatorSetStrategyRegistrationModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := ossr.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		ossr.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (ossr *OperatorSetStrategyRegistrationModel) formatMerkleLeafValue(
	strategy string,
	avs string,
	operatorSetId uint64,
	isActive bool,
) (string, error) {
	return fmt.Sprintf("%s_%s_%016x_%t", strategy, avs, operatorSetId, isActive), nil
}

func (ossr *OperatorSetStrategyRegistrationModel) sortValuesForMerkleTree(strategyRegistrations []*OperatorSetStrategyRegistration) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, strategyRegistration := range strategyRegistrations {
		slotID := base.NewSlotID(strategyRegistration.TransactionHash, strategyRegistration.LogIndex)
		value, err := ossr.formatMerkleLeafValue(
			strategyRegistration.Strategy,
			strategyRegistration.Avs,
			strategyRegistration.OperatorSetId,
			strategyRegistration.IsActive,
		)
		if err != nil {
			ossr.logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.String("strategy", strategyRegistration.Strategy),
				zap.String("avs", strategyRegistration.Avs),
				zap.Uint64("operatorSetId", strategyRegistration.OperatorSetId),
				zap.Bool("isActive", strategyRegistration.IsActive),
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

func (ossr *OperatorSetStrategyRegistrationModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return ossr.BaseEigenState.DeleteState("operator_set_strategy_registrations", startBlockNumber, endBlockNumber, ossr.DB)
}
