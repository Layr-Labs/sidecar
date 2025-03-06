package slashedOperators

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
	"math/big"
	"slices"
	"sort"
	"strings"
)

type SlashedOperator struct {
	Operator        string
	Strategy        string
	WadSlashed      string
	Description     string
	OperatorSetId   uint64
	Avs             string
	BlockNumber     uint64
	TransactionHash string
	LogIndex        uint64
}

type SlashedOperatorModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*SlashedOperator]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*SlashedOperator
	committedState   map[uint64][]*SlashedOperator
}

func NewSlashedOperatorModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*SlashedOperatorModel, error) {
	model := &SlashedOperatorModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*SlashedOperator),
		committedState:   make(map[uint64][]*SlashedOperator),
	}

	esm.RegisterState(model, 17)
	return model, nil
}

const SlashedOperatorModelName = "SlashedOperatorModel"

func (so *SlashedOperatorModel) GetModelName() string {
	return SlashedOperatorModelName
}

type slashedOperatorOutputData struct {
	Operator    string        `json:"operator"`
	Strategies  []string      `json:"strategies"`
	WadSlashed  []json.Number `json:"wadSlashed"`
	Description string        `json:"description"`
	OperatorSet struct {
		Id  uint64 `json:"id"`
		Avs string `json:"avs"`
	} `json:"operatorSet"`
}

func slotIdSuffix(operator string, strategy string, avs string, operatorSetId uint64) string {
	return fmt.Sprintf("%s_%s_%016x_%s", operator, strategy, operatorSetId, avs)
}

func (so *SlashedOperatorModel) NewSlotId(
	transactionHash string,
	logIndex uint64,
	operator string,
	strategy string,
	operatorSetId uint64,
	avs string,
) types.SlotID {
	return base.NewSlotIDWithSuffix(transactionHash, logIndex, slotIdSuffix(operator, strategy, avs, operatorSetId))
}

func parseSlashedOperatorOutputData(outputDataStr string) (*slashedOperatorOutputData, error) {
	outputData := &slashedOperatorOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (so *SlashedOperatorModel) handleSlashedOperatorCreatedEvent(log *storage.TransactionLog) ([]*SlashedOperator, error) {
	outputData, err := parseSlashedOperatorOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}
	slashedOperators := make([]*SlashedOperator, 0)

	for i, strategy := range outputData.Strategies {
		wadSlashedValue := outputData.WadSlashed[i]

		wadSlashed, success := new(big.Int).SetString(wadSlashedValue.String(), 10)
		if !success {
			err := fmt.Errorf("Failed to parse wadSlashed: %s", wadSlashedValue.String())
			so.logger.Sugar().Errorw("Failed to parse wadSlashed", zap.Error(err))
			return nil, err
		}

		slashing := &SlashedOperator{
			Operator:        strings.ToLower(outputData.Operator),
			Strategy:        strings.ToLower(strategy),
			WadSlashed:      wadSlashed.String(),
			Description:     outputData.Description,
			OperatorSetId:   outputData.OperatorSet.Id,
			Avs:             strings.ToLower(outputData.OperatorSet.Avs),
			BlockNumber:     log.BlockNumber,
			TransactionHash: log.TransactionHash,
			LogIndex:        log.LogIndex,
		}
		slashedOperators = append(slashedOperators, slashing)
	}

	return slashedOperators, nil
}

func (so *SlashedOperatorModel) GetStateTransitions() (types.StateTransitions[[]*SlashedOperator], []uint64) {
	stateChanges := make(types.StateTransitions[[]*SlashedOperator])

	stateChanges[0] = func(log *storage.TransactionLog) ([]*SlashedOperator, error) {
		createdEvents, err := so.handleSlashedOperatorCreatedEvent(log)
		if err != nil {
			return nil, err
		}

		for _, createdEvent := range createdEvents {
			slotId := so.NewSlotId(log.TransactionHash, log.LogIndex, createdEvent.Operator, createdEvent.Strategy, createdEvent.OperatorSetId, createdEvent.Avs)

			_, ok := so.stateAccumulator[log.BlockNumber][slotId]
			if ok {
				err := fmt.Errorf("Duplicate slashedOperator submitted for slot %s at block %d", slotId, log.BlockNumber)
				so.logger.Sugar().Errorw("Duplicate slashedOperator submitted", zap.Error(err))
				return nil, err
			}

			so.stateAccumulator[log.BlockNumber][slotId] = createdEvent
		}

		return createdEvents, nil
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

func (so *SlashedOperatorModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := so.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.AllocationManager: {
			"OperatorSlashed",
		},
	}
}

func (so *SlashedOperatorModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := so.getContractAddressesForEnvironment()
	return so.BaseEigenState.IsInterestingLog(addresses, log)
}

func (so *SlashedOperatorModel) SetupStateForBlock(blockNumber uint64) error {
	so.stateAccumulator[blockNumber] = make(map[types.SlotID]*SlashedOperator)
	so.committedState[blockNumber] = make([]*SlashedOperator, 0)
	return nil
}

func (so *SlashedOperatorModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(so.stateAccumulator, blockNumber)
	delete(so.committedState, blockNumber)
	return nil
}

func (so *SlashedOperatorModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := so.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			so.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (so *SlashedOperatorModel) prepareState(blockNumber uint64) ([]*SlashedOperator, error) {
	accumulatedState, ok := so.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		so.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*SlashedOperator, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (so *SlashedOperatorModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	recordsToInsert, err := so.prepareState(blockNumber)
	if err != nil {
		return err
	}

	insertedRecords, err := base.CommitFinalState(recordsToInsert, ignoreInsertConflicts, so.GetTableName(), so.DB)
	if err != nil {
		so.logger.Sugar().Errorw("Failed to commit final state", zap.Error(err))
		return err
	}
	so.committedState[blockNumber] = insertedRecords
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (so *SlashedOperatorModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := so.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := so.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := so.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		so.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (so *SlashedOperatorModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := so.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		so.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (so *SlashedOperatorModel) formatMerkleLeafValue(
	blockNumber uint64,
	wadSlashed string,
) (string, error) {
	return fmt.Sprintf("%016x", wadSlashed), nil
}

func (so *SlashedOperatorModel) sortValuesForMerkleTree(records []*SlashedOperator) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, record := range records {
		slotID := so.NewSlotId(record.TransactionHash, record.LogIndex, record.Operator, record.Strategy, record.OperatorSetId, record.Avs)
		value, err := so.formatMerkleLeafValue(record.BlockNumber, record.WadSlashed)
		if err != nil {
			so.logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.Uint64("blockNumber", record.BlockNumber),
				zap.String("operator", record.Operator),
				zap.String("strategy", record.Strategy),
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

func (so *SlashedOperatorModel) GetTableName() string {
	return "slashed_operators"
}

func (so *SlashedOperatorModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return so.BaseEigenState.DeleteState(so.GetTableName(), startBlockNumber, endBlockNumber, so.DB)
}

func (so *SlashedOperatorModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var splits []*SlashedOperator
	res := so.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&splits)
	if res.Error != nil {
		so.logger.Sugar().Errorw("Failed to list records", zap.Error(res.Error))
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(splits), nil
}

func (so *SlashedOperatorModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
