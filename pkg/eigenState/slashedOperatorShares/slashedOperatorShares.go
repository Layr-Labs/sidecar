package slashedOperatorShares

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

type SlashedOperatorShares struct {
	Operator           string
	Strategy           string
	TotalSlashedShares string
	BlockNumber        uint64
	TransactionHash    string
	LogIndex           uint64
}

type SlashedOperatorSharesModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[*SlashedOperatorShares]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*SlashedOperatorShares
	committedState   map[uint64][]*SlashedOperatorShares
}

func NewSlashedOperatorSharesModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*SlashedOperatorSharesModel, error) {
	model := &SlashedOperatorSharesModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*SlashedOperatorShares),
		committedState:   make(map[uint64][]*SlashedOperatorShares),
	}

	esm.RegisterState(model, 20)
	return model, nil
}

func (sos *SlashedOperatorSharesModel) GetModelName() string {
	return "SlashedOperatorShares"
}

type slashedOperatorSharesOutputData struct {
	Strategy           string      `json:"strategy"`
	TotalSlashedShares json.Number `json:"totalSlashedShares"`
}

func parseSlashedOperatorSharesOutputData(outputDataStr string) (*slashedOperatorSharesOutputData, error) {
	outputData := &slashedOperatorSharesOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (sos *SlashedOperatorSharesModel) handleSlashedOperatorSharesCreatedEvent(log *storage.TransactionLog) (*SlashedOperatorShares, error) {
	arguments, err := sos.ParseLogArguments(log)
	if err != nil {
		return nil, err
	}

	outputData, err := parseSlashedOperatorSharesOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}

	sharesSlashed, success := new(big.Int).SetString(outputData.TotalSlashedShares.String(), 10)
	if !success {
		err := fmt.Errorf("Failed to parse totalSlashedShares: %s", outputData.TotalSlashedShares.String())
		sos.logger.Sugar().Errorw("Failed to parse totalSlashedShares", zap.Error(err))
		return nil, err
	}

	split := &SlashedOperatorShares{
		Operator:           strings.ToLower(arguments[0].Value.(string)),
		Strategy:           strings.ToLower(outputData.Strategy),
		TotalSlashedShares: sharesSlashed.String(),
		BlockNumber:        log.BlockNumber,
		TransactionHash:    log.TransactionHash,
		LogIndex:           log.LogIndex,
	}

	return split, nil
}

func (sos *SlashedOperatorSharesModel) GetStateTransitions() (types.StateTransitions[*SlashedOperatorShares], []uint64) {
	stateChanges := make(types.StateTransitions[*SlashedOperatorShares])

	stateChanges[0] = func(log *storage.TransactionLog) (*SlashedOperatorShares, error) {
		createdEvent, err := sos.handleSlashedOperatorSharesCreatedEvent(log)
		if err != nil {
			return nil, err
		}

		slotId := base.NewSlotID(createdEvent.TransactionHash, createdEvent.LogIndex)

		_, ok := sos.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate slashedOperatorShares submitted for slot %s at block %d", slotId, log.BlockNumber)
			sos.logger.Sugar().Errorw("Duplicate slashedOperatorShares submitted", zap.Error(err))
			return nil, err
		}

		sos.stateAccumulator[log.BlockNumber][slotId] = createdEvent

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

func (sos *SlashedOperatorSharesModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := sos.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.DelegationManager: {
			"OperatorSharesSlashed",
		},
	}
}

func (sos *SlashedOperatorSharesModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := sos.getContractAddressesForEnvironment()
	return sos.BaseEigenState.IsInterestingLog(addresses, log)
}

func (sos *SlashedOperatorSharesModel) SetupStateForBlock(blockNumber uint64) error {
	sos.stateAccumulator[blockNumber] = make(map[types.SlotID]*SlashedOperatorShares)
	sos.committedState[blockNumber] = make([]*SlashedOperatorShares, 0)
	return nil
}

func (sos *SlashedOperatorSharesModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(sos.stateAccumulator, blockNumber)
	delete(sos.committedState, blockNumber)
	return nil
}

func (sos *SlashedOperatorSharesModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := sos.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			sos.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (sos *SlashedOperatorSharesModel) prepareState(blockNumber uint64) ([]*SlashedOperatorShares, error) {
	accumulatedState, ok := sos.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		sos.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*SlashedOperatorShares, 0)
	for _, split := range accumulatedState {
		recordsToInsert = append(recordsToInsert, split)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (sos *SlashedOperatorSharesModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	recordsToInsert, err := sos.prepareState(blockNumber)
	if err != nil {
		return err
	}

	insertedRecords, err := base.CommitFinalState(recordsToInsert, ignoreInsertConflicts, sos.GetTableName(), sos.DB)
	if err != nil {
		sos.logger.Sugar().Errorw("Failed to commit final state", zap.Error(err))
		return err
	}
	sos.committedState[blockNumber] = insertedRecords
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (sos *SlashedOperatorSharesModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := sos.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := sos.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := sos.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		sos.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (sos *SlashedOperatorSharesModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := sos.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		sos.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (sos *SlashedOperatorSharesModel) formatMerkleLeafValue(
	blockNumber uint64,
	operator string,
	strategy string,
	totalSlashedShares string,
) (string, error) {
	return fmt.Sprintf("%s_%s_%s", operator, strategy, totalSlashedShares), nil
}

func (sos *SlashedOperatorSharesModel) sortValuesForMerkleTree(records []*SlashedOperatorShares) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, record := range records {
		slotID := base.NewSlotID(record.TransactionHash, record.LogIndex)
		value, err := sos.formatMerkleLeafValue(record.BlockNumber, record.Operator, record.Strategy, record.TotalSlashedShares)
		if err != nil {
			sos.logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.Uint64("blockNumber", record.BlockNumber),
				zap.String("operator", record.Operator),
				zap.String("strategy", record.Strategy),
				zap.String("totalSlashedShares", record.TotalSlashedShares),
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

func (sos *SlashedOperatorSharesModel) GetTableName() string {
	return "slashed_operator_shares"
}

func (sos *SlashedOperatorSharesModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return sos.BaseEigenState.DeleteState(sos.GetTableName(), startBlockNumber, endBlockNumber, sos.DB)
}

func (sos *SlashedOperatorSharesModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var splits []*SlashedOperatorShares
	res := sos.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&splits)
	if res.Error != nil {
		sos.logger.Sugar().Errorw("Failed to list records", zap.Error(res.Error))
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(splits), nil
}

func (sos *SlashedOperatorSharesModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
