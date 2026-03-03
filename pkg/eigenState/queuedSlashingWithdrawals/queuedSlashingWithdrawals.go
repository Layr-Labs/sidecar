package queuedSlashingWithdrawals

import (
	"encoding/hex"
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

type QueuedSlashingWithdrawal struct {
	Staker           string
	Operator         string
	Withdrawer       string
	Nonce            string
	StartBlock       uint64
	Strategy         string
	ScaledShares     string
	SharesToWithdraw string
	WithdrawalRoot   string
	BlockNumber      uint64
	TransactionHash  string
	LogIndex         uint64
}

type QueuedSlashingWithdrawalModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[*QueuedSlashingWithdrawal]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*QueuedSlashingWithdrawal
	committedState   map[uint64][]*QueuedSlashingWithdrawal
}

func NewQueuedSlashingWithdrawalModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*QueuedSlashingWithdrawalModel, error) {
	model := &QueuedSlashingWithdrawalModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*QueuedSlashingWithdrawal),
		committedState:   make(map[uint64][]*QueuedSlashingWithdrawal),
	}

	esm.RegisterState(model, 22)
	return model, nil
}

const QueuedSlashingWithdrawalModelName = "QueuedSlashingWithdrawalModel"

func (omm *QueuedSlashingWithdrawalModel) GetModelName() string {
	return QueuedSlashingWithdrawalModelName
}

type queuedSlashingWithdrawalOutputData struct {
	Withdrawal struct {
		Nonce        json.Number   `json:"nonce"`
		Staker       string        `json:"staker"`
		StartBlock   uint64        `json:"startBlock"`
		Strategies   []string      `json:"strategies"`
		Withdrawer   string        `json:"withdrawer"`
		DelegatedTo  string        `json:"delegatedTo"`
		ScaledShares []json.Number `json:"scaledShares"`
	} `json:"withdrawal"`
	WithdrawalRoot   []byte        `json:"withdrawalRoot"`
	SharesToWithdraw []json.Number `json:"sharesToWithdraw"`
}

func parseQueuedSlashingWithdrawalOutputData(outputDataStr string) (*queuedSlashingWithdrawalOutputData, error) {
	outputData := &queuedSlashingWithdrawalOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (omm *QueuedSlashingWithdrawalModel) NewSlotId(transactionHash string, logIndex uint64, staker string, operator string, strategy string) types.SlotID {
	suffix := fmt.Sprintf("%s_%s_%s", staker, operator, strategy)
	return base.NewSlotIDWithSuffix(transactionHash, logIndex, suffix)
}

func (omm *QueuedSlashingWithdrawalModel) GetStateTransitions() (types.StateTransitions[[]*QueuedSlashingWithdrawal], []uint64) {
	stateChanges := make(types.StateTransitions[[]*QueuedSlashingWithdrawal])

	stateChanges[0] = func(log *storage.TransactionLog) ([]*QueuedSlashingWithdrawal, error) {
		outputData, err := parseQueuedSlashingWithdrawalOutputData(log.OutputData)
		if err != nil {
			return nil, err
		}

		withdrawalRoot := hex.EncodeToString(outputData.WithdrawalRoot)

		createdEvents := make([]*QueuedSlashingWithdrawal, 0)
		for i, strategy := range outputData.Withdrawal.Strategies {
			event := &QueuedSlashingWithdrawal{
				Staker:           strings.ToLower(outputData.Withdrawal.Staker),
				Operator:         strings.ToLower(outputData.Withdrawal.DelegatedTo),
				Withdrawer:       strings.ToLower(outputData.Withdrawal.Withdrawer),
				Nonce:            outputData.Withdrawal.Nonce.String(),
				StartBlock:       outputData.Withdrawal.StartBlock,
				Strategy:         strings.ToLower(strategy),
				ScaledShares:     outputData.Withdrawal.ScaledShares[i].String(),
				WithdrawalRoot:   withdrawalRoot,
				SharesToWithdraw: outputData.SharesToWithdraw[i].String(),
				BlockNumber:      log.BlockNumber,
				TransactionHash:  log.TransactionHash,
				LogIndex:         log.LogIndex,
			}

			slotId := omm.NewSlotId(event.TransactionHash, event.LogIndex, event.Staker, event.Operator, event.Strategy)
			_, ok := omm.stateAccumulator[log.BlockNumber][slotId]
			if ok {
				err := fmt.Errorf("Duplicate queuedSlashingWithdrawal submitted for slot %s at block %d", slotId, log.BlockNumber)
				omm.logger.Sugar().Errorw("Duplicate queuedSlashingWithdrawal submitted", zap.Error(err))
				return nil, err
			}

			omm.stateAccumulator[log.BlockNumber][slotId] = event
			createdEvents = append(createdEvents, event)
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

func (omm *QueuedSlashingWithdrawalModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := omm.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.DelegationManager: {
			"SlashingWithdrawalQueued",
		},
	}
}

func (omm *QueuedSlashingWithdrawalModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := omm.getContractAddressesForEnvironment()
	return omm.BaseEigenState.IsInterestingLog(addresses, log)
}

func (omm *QueuedSlashingWithdrawalModel) SetupStateForBlock(blockNumber uint64) error {
	omm.stateAccumulator[blockNumber] = make(map[types.SlotID]*QueuedSlashingWithdrawal)
	omm.committedState[blockNumber] = make([]*QueuedSlashingWithdrawal, 0)
	return nil
}

func (omm *QueuedSlashingWithdrawalModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(omm.stateAccumulator, blockNumber)
	delete(omm.committedState, blockNumber)
	return nil
}

func (omm *QueuedSlashingWithdrawalModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
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
func (omm *QueuedSlashingWithdrawalModel) prepareState(blockNumber uint64) ([]*QueuedSlashingWithdrawal, error) {
	accumulatedState, ok := omm.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		omm.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*QueuedSlashingWithdrawal, 0)
	for _, record := range accumulatedState {
		recordsToInsert = append(recordsToInsert, record)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (omm *QueuedSlashingWithdrawalModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	recordsToInsert, err := omm.prepareState(blockNumber)
	if err != nil {
		return err
	}

	insertedRecords, err := base.CommitFinalState(recordsToInsert, ignoreInsertConflicts, omm.GetTableName(), omm.DB)
	if err != nil {
		omm.logger.Sugar().Errorw("Failed to commit final state",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("recordsToInsert", recordsToInsert),
		)
		return err
	}
	omm.committedState[blockNumber] = insertedRecords
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (omm *QueuedSlashingWithdrawalModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
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

func (omm *QueuedSlashingWithdrawalModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := omm.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		omm.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (omm *QueuedSlashingWithdrawalModel) formatMerkleLeafValue(
	blockNumber uint64,
	staker string,
	operator string,
	strategy string,
	sharesToWithdraw string,
) (string, error) {
	return fmt.Sprintf("%s_%s_%s_%016x", staker, operator, strategy, sharesToWithdraw), nil
}

func (omm *QueuedSlashingWithdrawalModel) sortValuesForMerkleTree(records []*QueuedSlashingWithdrawal) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, record := range records {
		slotID := omm.NewSlotId(record.TransactionHash, record.LogIndex, record.Staker, record.Operator, record.Strategy)
		value, err := omm.formatMerkleLeafValue(record.BlockNumber, record.Staker, record.Operator, record.Strategy, record.SharesToWithdraw)
		if err != nil {
			omm.logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.Uint64("blockNumber", record.BlockNumber),
				zap.String("staker", record.Staker),
				zap.String("operator", record.Operator),
				zap.String("strategy", record.Strategy),
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

func (omm *QueuedSlashingWithdrawalModel) GetTableName() string {
	return "queued_slashing_withdrawals"
}

func (omm *QueuedSlashingWithdrawalModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return omm.BaseEigenState.DeleteState(omm.GetTableName(), startBlockNumber, endBlockNumber, omm.DB)
}

func (omm *QueuedSlashingWithdrawalModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var splits []*QueuedSlashingWithdrawal
	res := omm.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&splits)
	if res.Error != nil {
		omm.logger.Sugar().Errorw("Failed to list records", zap.Error(res.Error))
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(splits), nil
}

func (omm *QueuedSlashingWithdrawalModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	forks, err := omm.globalConfig.GetRewardsSqlForkDates()
	if err != nil {
		omm.logger.Sugar().Errorw("Failed to get rewards sql fork dates", zap.Error(err))
		return false, err
	}

	return blockHeight >= forks[config.RewardsFork_Red].BlockNumber, nil
}
