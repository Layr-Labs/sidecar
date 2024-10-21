package withdrawals

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/base"
	m2DelegationManager "github.com/Layr-Labs/go-sidecar/internal/eigenState/bindings/DelegationManagerM2"
	slashingDelegationManager "github.com/Layr-Labs/go-sidecar/internal/eigenState/bindings/DelegationManagerSlashing"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/types"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"github.com/Layr-Labs/go-sidecar/internal/utils"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ShareWithdrawal struct {
	Staker   string
	Operator string
	Strategy string
	Shares   string

	Nonce string
	Root  string

	QueueBlockNumber uint64
	QueueBlockTime   time.Time

	Completed            bool
	CompletedBlockNumber uint64
	CompletedBlockTime   time.Time
}

func NewSlotID(staker string, nonce string) types.SlotID {
	return types.SlotID(fmt.Sprintf("%s_%s", staker, nonce))
}

type WithdrawalsModel struct {
	base.BaseEigenState
	DB           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64][]*ShareWithdrawal
}

func NewWithdrawalsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*WithdrawalsModel, error) {
	model := &WithdrawalsModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64][]*ShareWithdrawal),
	}

	esm.RegisterState(model, 69)

	return model, nil
}

func (w *WithdrawalsModel) GetModelName() string {
	return "WithdrawalsModel"
}

func (w *WithdrawalsModel) parseWithdrawalQueuedLog(log *storage.TransactionLog) (*ShareWithdrawals, error) {
	withdrawalQueued := &m2DelegationManager.DelegationManagerWithdrawalQueued{}
	logBytes, err := log.CombineArgs()
	if err != nil {
		return nil, xerrors.Errorf("failed to combine log arguments: %w", err)
	}

	err = json.Unmarshal(logBytes, withdrawalQueued)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal WithdrawalQueued log: %w", err)
	}

	shareWithdrawals := make([]*ShareWithdrawal, len(withdrawalQueued.Withdrawal.Strategies))
	for i, strategy := range withdrawalQueued.Withdrawal.Strategies {
		shareWithdrawals[i] = &ShareWithdrawal{
			Staker:           withdrawalQueued.Withdrawal.Staker.String(),
			Operator:         withdrawalQueued.Withdrawal.DelegatedTo.String(),
			Strategy:         strategy.String(),
			Shares:           withdrawalQueued.Withdrawal.Shares[i].String(),
			Nonce:            withdrawalQueued.Withdrawal.Nonce.String(),
			Root:             hex.EncodeToString(withdrawalQueued.WithdrawalRoot[:]),
			QueueBlockNumber: log.BlockNumber,
			QueueBlockTime:   log.BlockTime,
			Completed:        false,
		}
	}

	return &ShareWithdrawals{ShareWithdrawals: shareWithdrawals}, nil
}

func (w *WithdrawalsModel) parseWithdrawalCompletedLog(log *storage.TransactionLog) (*ShareWithdrawals, error) {
	withdrawalCompleted := &m2DelegationManager.DelegationManagerWithdrawalCompleted{}
	logBytes, err := log.CombineArgs()
	if err != nil {
		return nil, xerrors.Errorf("failed to combine log arguments: %w", err)
	}

	err = json.Unmarshal(logBytes, withdrawalCompleted)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal WithdrawalCompleted log: %w", err)
	}

	shareWithdrawals := make([]*ShareWithdrawal, 1)
	shareWithdrawals[0] = &ShareWithdrawal{
		Root:                 hex.EncodeToString(withdrawalCompleted.WithdrawalRoot[:]),
		Completed:            true,
		CompletedBlockNumber: log.BlockNumber,
		CompletedBlockTime:   log.BlockTime,
	}

	return &ShareWithdrawals{ShareWithdrawals: shareWithdrawals}, nil
}

func (w *WithdrawalsModel) parseSlashingWithdrawalQueuedLog(log *storage.TransactionLog) (*ShareWithdrawals, error) {
	slashingWithdrawalQueued := &slashingDelegationManager.DelegationManagerSlashingWithdrawalQueued{}
	logBytes, err := log.CombineArgs()
	if err != nil {
		return nil, xerrors.Errorf("failed to combine log arguments: %w", err)
	}

	err = json.Unmarshal(logBytes, slashingWithdrawalQueued)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal SlashingWithdrawalQueued log: %w", err)
	}

	shareWithdrawals := make([]*ShareWithdrawal, len(slashingWithdrawalQueued.Withdrawal.Strategies))
	for i, strategy := range slashingWithdrawalQueued.Withdrawal.Strategies {
		shareWithdrawals[i] = &ShareWithdrawal{
			Staker:           slashingWithdrawalQueued.Withdrawal.Staker.String(),
			Operator:         slashingWithdrawalQueued.Withdrawal.DelegatedTo.String(),
			Strategy:         strategy.String(),
			Shares:           slashingWithdrawalQueued.SharesToWithdraw[i].String(),
			Nonce:            slashingWithdrawalQueued.Withdrawal.Nonce.String(),
			Root:             hex.EncodeToString(slashingWithdrawalQueued.WithdrawalRoot[:]),
			QueueBlockNumber: log.BlockNumber,
			QueueBlockTime:   log.BlockTime,
			Completed:        false,
		}
	}

	return &ShareWithdrawals{ShareWithdrawals: shareWithdrawals}, nil
}

func (w *WithdrawalsModel) parseSlashingWithdrawalCompletedLog(log *storage.TransactionLog) (*ShareWithdrawals, error) {
	slashingWithdrawalCompleted := &slashingDelegationManager.DelegationManagerSlashingWithdrawalCompleted{}
	logBytes, err := log.CombineArgs()
	if err != nil {
		return nil, xerrors.Errorf("failed to combine log arguments: %w", err)
	}

	err = json.Unmarshal(logBytes, slashingWithdrawalCompleted)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal SlashingWithdrawalCompleted log: %w", err)
	}

	shareWithdrawals := make([]*ShareWithdrawal, 1)
	shareWithdrawals[0] = &ShareWithdrawal{
		Root:                 hex.EncodeToString(slashingWithdrawalCompleted.WithdrawalRoot[:]),
		Completed:            true,
		CompletedBlockNumber: log.BlockNumber,
		CompletedBlockTime:   log.BlockTime,
	}

	return &ShareWithdrawals{ShareWithdrawals: shareWithdrawals}, nil
}

type ShareWithdrawals struct {
	ShareWithdrawals []*ShareWithdrawal
}

func (w *WithdrawalsModel) GetStateTransitions() ([]uint64, types.StateTransitions[ShareWithdrawals]) {
	stateChanges := make(types.StateTransitions[ShareWithdrawals])

	stateChanges[0] = func(log *storage.TransactionLog) (*ShareWithdrawals, error) {
		shareWithdrawalsForLog := &ShareWithdrawals{}
		var err error
		switch log.EventName {
		case "WithdrawalQueued":
			shareWithdrawalsForLog, err = w.parseWithdrawalQueuedLog(log)
		case "WithdrawalCompleted":
			shareWithdrawalsForLog, err = w.parseWithdrawalCompletedLog(log)
		case "SlashingWithdrawalQueued":
			shareWithdrawalsForLog, err = w.parseSlashingWithdrawalQueuedLog(log)
		case "SlashingWithdrawalCompleted":
			shareWithdrawalsForLog, err = w.parseSlashingWithdrawalCompletedLog(log)
		}

		if err != nil {
			// include event name in error message
			return nil, xerrors.Errorf("error parsing %s log: %w", log.EventName, err)
		}

		return shareWithdrawalsForLog, nil
	}

	// sort the fork block numbers in descending order
	forkBlockNumbers := make([]uint64, 0)
	for blockNumber := range stateChanges {
		forkBlockNumbers = append(forkBlockNumbers, blockNumber)
	}
	sort.Slice(forkBlockNumbers, func(i, j int) bool {
		return forkBlockNumbers[i] > forkBlockNumbers[j]
	})

	return forkBlockNumbers, stateChanges
}

func (w *WithdrawalsModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := w.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.DelegationManager: {
			"WithdrawalQueued",
			"WithdrawalCompleted",
			"SlashingWithdrawalQueued",
			"SlashingWithdrawalCompleted",
		},
	}
}

func (w *WithdrawalsModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := w.getContractAddressesForEnvironment()
	return w.BaseEigenState.IsInterestingLog(addresses, log)
}

func (w *WithdrawalsModel) SetupStateForBlock(blockNumber uint64) error {
	w.stateAccumulator[blockNumber] = make([]*ShareWithdrawal, 0)
	return nil
}

func (w *WithdrawalsModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(w.stateAccumulator, blockNumber)
	return nil
}

func (w *WithdrawalsModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	sortedBlockNumbers, stateChanges := w.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			w.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", blockNumber))

			change, err := stateChanges[blockNumber](log)
			if err != nil {
				return nil, err
			}
			if change == nil {
				return nil, xerrors.Errorf("No state change found for block %d", blockNumber)
			}
			return change, nil
		}
	}
	return nil, nil
}

func (w *WithdrawalsModel) prepareState(blockNumber uint64) ([]*ShareWithdrawal, []*ShareWithdrawal, error) {
	shareWithdrawals := w.stateAccumulator[blockNumber]
	if shareWithdrawals == nil {
		return nil, nil, errors.New("no state changes found for block")
	}

	inserts := make([]*ShareWithdrawal, 0)
	updates := make([]*ShareWithdrawal, 0)

	for _, shareWithdrawal := range shareWithdrawals {
		if !shareWithdrawal.Completed {
			inserts = append(inserts, shareWithdrawal)
		} else {
			updates = append(updates, shareWithdrawal)
		}
	}

	return inserts, updates, nil
}

func (w *WithdrawalsModel) CommitFinalState(blockNumber uint64) error {
	inserts, updates, err := w.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(inserts) > 0 {
		res := w.DB.Model(&ShareWithdrawal{}).Clauses(clause.Returning{}).Create(&inserts)
		if res.Error != nil {
			return err
		}
	}

	if len(updates) > 0 {
		completedRoots := make([]string, 0)
		for _, update := range updates {
			completedRoots = append(completedRoots, update.Root)
		}

		// update the complete columns of each share withdrawal
		// can batch update all because they're all at the same block number and timestamp
		res := w.DB.Model(&ShareWithdrawal{}).Where("root IN ?", completedRoots).Updates(map[string]interface{}{
			"completed":              updates[0].Completed,
			"completed_block_number": updates[0].CompletedBlockNumber,
			"completed_block_time":   updates[0].CompletedBlockTime,
		})
		if res.Error != nil {
			return err
		}
	}

	return nil
}

func (w *WithdrawalsModel) GenerateStateRoot(blockNumber uint64) (types.StateRoot, error) {
	diffs := w.stateAccumulator[blockNumber]
	if diffs == nil {
		return "", errors.New("no state changes found for block")
	}

	inputs := make([]*base.MerkleTreeInput, 0)
	for _, diff := range diffs {
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: NewSlotID(diff.Staker, diff.Nonce),
			Value:  []byte(diff.Root),
		})
	}

	// sort by slotId
	sort.Slice(inputs, func(i, j int) bool {
		return inputs[i].SlotID < inputs[j].SlotID
	})

	fullTree, err := w.MerkleizeState(blockNumber, inputs)
	if err != nil {
		return "", err
	}
	return types.StateRoot(utils.ConvertBytesToString(fullTree.Root())), nil
}

func (w *WithdrawalsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return w.BaseEigenState.DeleteState("share_withdrawals", startBlockNumber, endBlockNumber, w.DB)
}
