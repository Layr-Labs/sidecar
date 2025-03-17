package completedSlashingWithdrawals

import (
	"encoding/hex"
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

type CompletedSlashingWithdrawal struct {
	WithdrawalRoot  string
	BlockNumber     uint64
	TransactionHash string
	LogIndex        uint64
}

type CompletedSlashingWithdrawalModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[*CompletedSlashingWithdrawal]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*CompletedSlashingWithdrawal
	committedState   map[uint64][]*CompletedSlashingWithdrawal
}

func NewCompletedSlashingWithdrawalModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*CompletedSlashingWithdrawalModel, error) {
	model := &CompletedSlashingWithdrawalModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*CompletedSlashingWithdrawal),
		committedState:   make(map[uint64][]*CompletedSlashingWithdrawal),
	}

	esm.RegisterState(model, 23)
	return model, nil
}

const CompletedSlashingWithdrawalModelName = "CompletedSlashingWithdrawalModel"

func (omm *CompletedSlashingWithdrawalModel) GetModelName() string {
	return CompletedSlashingWithdrawalModelName
}

type completedSlashingWithdrawalOutputData struct {
	WithdrawalRoot []byte `json:"withdrawalRoot"`
}

func parseCompletedSlashingWithdrawalOutputData(outputDataStr string) (*completedSlashingWithdrawalOutputData, error) {
	outputData := &completedSlashingWithdrawalOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (omm *CompletedSlashingWithdrawalModel) NewSlotId(transactionHash string, logIndex uint64, staker string, operator string, strategy string) types.SlotID {
	suffix := fmt.Sprintf("%s_%s_%s", staker, operator, strategy)
	return base.NewSlotIDWithSuffix(transactionHash, logIndex, suffix)
}

func (omm *CompletedSlashingWithdrawalModel) GetStateTransitions() (types.StateTransitions[*CompletedSlashingWithdrawal], []uint64) {
	stateChanges := make(types.StateTransitions[*CompletedSlashingWithdrawal])

	stateChanges[0] = func(log *storage.TransactionLog) (*CompletedSlashingWithdrawal, error) {
		outputData, err := parseCompletedSlashingWithdrawalOutputData(log.OutputData)
		if err != nil {
			return nil, err
		}

		withdrawalRoot := hex.EncodeToString(outputData.WithdrawalRoot)

		event := &CompletedSlashingWithdrawal{
			WithdrawalRoot:  withdrawalRoot,
			BlockNumber:     log.BlockNumber,
			TransactionHash: log.TransactionHash,
			LogIndex:        log.LogIndex,
		}

		slotId := base.NewSlotID(log.TransactionHash, log.LogIndex)
		_, ok := omm.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate completedSlashingWithdrawal submitted for slot %s at block %d", slotId, log.BlockNumber)
			omm.logger.Sugar().Errorw("Duplicate completedSlashingWithdrawal submitted", zap.Error(err))
			return nil, err
		}

		omm.stateAccumulator[log.BlockNumber][slotId] = event

		return event, nil
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

func (omm *CompletedSlashingWithdrawalModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := omm.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.DelegationManager: {
			"SlashingWithdrawalCompleted",
		},
	}
}

func (omm *CompletedSlashingWithdrawalModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := omm.getContractAddressesForEnvironment()
	return omm.BaseEigenState.IsInterestingLog(addresses, log)
}

func (omm *CompletedSlashingWithdrawalModel) SetupStateForBlock(blockNumber uint64) error {
	omm.stateAccumulator[blockNumber] = make(map[types.SlotID]*CompletedSlashingWithdrawal)
	omm.committedState[blockNumber] = make([]*CompletedSlashingWithdrawal, 0)
	return nil
}

func (omm *CompletedSlashingWithdrawalModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(omm.stateAccumulator, blockNumber)
	delete(omm.committedState, blockNumber)
	return nil
}

func (omm *CompletedSlashingWithdrawalModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := omm.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			omm.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (omm *CompletedSlashingWithdrawalModel) prepareState(blockNumber uint64) ([]*CompletedSlashingWithdrawal, error) {
	accumulatedState, ok := omm.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		omm.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*CompletedSlashingWithdrawal, 0)
	for _, record := range accumulatedState {
		recordsToInsert = append(recordsToInsert, record)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (omm *CompletedSlashingWithdrawalModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	recordsToInsert, err := omm.prepareState(blockNumber)
	if err != nil {
		return err
	}

	insertedRecords, err := base.CommitFinalState(recordsToInsert, ignoreInsertConflicts, omm.GetTableName(), omm.DB)
	if err != nil {
		omm.logger.Sugar().Errorw("Failed to commit final state", zap.Error(err))
		return err
	}
	omm.committedState[blockNumber] = insertedRecords
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (omm *CompletedSlashingWithdrawalModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := omm.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := omm.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := omm.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		omm.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (omm *CompletedSlashingWithdrawalModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := omm.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		omm.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (omm *CompletedSlashingWithdrawalModel) formatMerkleLeafValue(
	blockNumber uint64,
	withdrawalRoot string,
) (string, error) {
	return withdrawalRoot, nil
}

func (omm *CompletedSlashingWithdrawalModel) sortValuesForMerkleTree(records []*CompletedSlashingWithdrawal) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, record := range records {
		slotID := base.NewSlotID(record.TransactionHash, record.LogIndex)
		value, err := omm.formatMerkleLeafValue(record.BlockNumber, record.WithdrawalRoot)
		if err != nil {
			omm.logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.Uint64("blockNumber", record.BlockNumber),
				zap.String("transactionHash", record.TransactionHash),
				zap.Uint64("logIndex", record.LogIndex),
				zap.String("withdrawalRoot", record.WithdrawalRoot),
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

func (omm *CompletedSlashingWithdrawalModel) GetTableName() string {
	return "completed_slashing_withdrawals"
}

func (omm *CompletedSlashingWithdrawalModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return omm.BaseEigenState.DeleteState(omm.GetTableName(), startBlockNumber, endBlockNumber, omm.DB)
}

func (omm *CompletedSlashingWithdrawalModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var splits []*CompletedSlashingWithdrawal
	res := omm.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&splits)
	if res.Error != nil {
		omm.logger.Sugar().Errorw("Failed to list records", zap.Error(res.Error))
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(splits), nil
}

func (omm *CompletedSlashingWithdrawalModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	forks, err := omm.globalConfig.GetRewardsSqlForkDates()
	if err != nil {
		omm.logger.Sugar().Errorw("Failed to get rewards sql fork dates", zap.Error(err))
		return false, err
	}

	return blockHeight >= forks[config.RewardsFork_Brazos].BlockNumber, nil
}
