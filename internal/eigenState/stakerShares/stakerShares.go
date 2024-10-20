package stakerShares

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/base"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/types"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"github.com/Layr-Labs/go-sidecar/internal/types/numbers"
	pkgUtils "github.com/Layr-Labs/go-sidecar/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type StakerShares struct {
	Staker      string
	Strategy    string
	Shares      string
	BlockNumber uint64
	CreatedAt   time.Time
}

type AccumulatedStateChange struct {
	Staker      string
	Strategy    string
	Shares      *big.Int
	BlockNumber uint64
}

func NewSlotID(staker string, strategy string) types.SlotID {
	return types.SlotID(fmt.Sprintf("%s_%s", staker, strategy))
}

type StakerSharesModel struct {
	base.BaseEigenState
	DB           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*AccumulatedStateChange
}

func NewStakerSharesModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*StakerSharesModel, error) {
	model := &StakerSharesModel{
		BaseEigenState:   base.NewBaseEigenState(logger, grm),
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*AccumulatedStateChange),
	}

	esm.RegisterState(model, 3)
	return model, nil
}

func (ss *StakerSharesModel) GetModelName() string {
	return "StakerShares"
}

type depositOutputData struct {
	Depositor string      `json:"depositor"`
	Staker    string      `json:"staker"`
	Strategy  string      `json:"strategy"`
	Shares    json.Number `json:"shares"`
}

// parseLogOutputForDepositEvent parses the output data of a Deposit event
// Custom parser to preserve the precision of the shares value.
// Allowing the standard json.Unmarshal to parse the shares value to a float64 which
// causes it to lose precision by being represented as scientific notation.
func parseLogOutputForDepositEvent(outputDataStr string) (*depositOutputData, error) {
	outputData := &depositOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}
	outputData.Staker = strings.ToLower(outputData.Staker)
	outputData.Depositor = strings.ToLower(outputData.Depositor)
	outputData.Strategy = strings.ToLower(outputData.Strategy)
	return outputData, err
}

func (ss *StakerSharesModel) handleStakerDepositEvent(log *storage.TransactionLog) (*AccumulatedStateChange, error) {
	outputData, err := parseLogOutputForDepositEvent(log.OutputData)
	if err != nil {
		return nil, err
	}

	var stakerAddress string
	if outputData.Depositor != "" {
		stakerAddress = outputData.Depositor
	}
	if outputData.Staker != "" {
		stakerAddress = outputData.Staker
	}

	if stakerAddress == "" {
		return nil, xerrors.Errorf("No staker address found in event")
	}

	shares, success := numbers.NewBig257().SetString(outputData.Shares.String(), 10)
	if !success {
		return nil, xerrors.Errorf("Failed to convert shares to big.Int: %s", outputData.Shares)
	}

	return &AccumulatedStateChange{
		Staker:      stakerAddress,
		Strategy:    outputData.Strategy,
		Shares:      shares,
		BlockNumber: log.BlockNumber,
	}, nil
}

type podSharesUpdatedOutputData struct {
	SharesDelta json.Number `json:"sharesDelta"`
}

func parseLogOutputForPodSharesUpdatedEvent(outputDataStr string) (*podSharesUpdatedOutputData, error) {
	outputData := &podSharesUpdatedOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}
	return outputData, err
}

func (ss *StakerSharesModel) handlePodSharesUpdatedEvent(log *storage.TransactionLog) (*AccumulatedStateChange, error) {
	arguments, err := ss.ParseLogArguments(log)
	if err != nil {
		return nil, err
	}
	outputData, err := parseLogOutputForPodSharesUpdatedEvent(log.OutputData)
	if err != nil {
		return nil, err
	}

	staker := strings.ToLower(arguments[0].Value.(string))

	sharesDeltaStr := outputData.SharesDelta.String()

	sharesDelta, success := numbers.NewBig257().SetString(sharesDeltaStr, 10)
	if !success {
		return nil, xerrors.Errorf("Failed to convert shares to big.Int: %s", sharesDelta)
	}

	return &AccumulatedStateChange{
		Staker:      staker,
		Strategy:    "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0",
		Shares:      sharesDelta,
		BlockNumber: log.BlockNumber,
	}, nil
}

func (ss *StakerSharesModel) handleM1StakerWithdrawals(log *storage.TransactionLog) (*AccumulatedStateChange, error) {
	outputData, err := parseLogOutputForDepositEvent(log.OutputData)
	if err != nil {
		return nil, err
	}

	var stakerAddress string
	if outputData.Depositor != "" {
		stakerAddress = outputData.Depositor
	}
	if outputData.Staker != "" {
		stakerAddress = outputData.Staker
	}

	if stakerAddress == "" {
		return nil, xerrors.Errorf("No staker address found in event")
	}

	shares, success := numbers.NewBig257().SetString(outputData.Shares.String(), 10)
	if !success {
		return nil, xerrors.Errorf("Failed to convert shares to big.Int: %s", outputData.Shares)
	}

	return &AccumulatedStateChange{
		Staker:      stakerAddress,
		Strategy:    outputData.Strategy,
		Shares:      shares.Mul(shares, big.NewInt(-1)),
		BlockNumber: log.BlockNumber,
	}, nil
}

type m2MigrationOutputData struct {
	OldWithdrawalRoot       []byte `json:"oldWithdrawalRoot"`
	OldWithdrawalRootString string
}

func parseLogOutputForM2MigrationEvent(outputDataStr string) (*m2MigrationOutputData, error) {
	outputData := &m2MigrationOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}
	outputData.OldWithdrawalRootString = hex.EncodeToString(outputData.OldWithdrawalRoot)
	return outputData, err
}

// handleMigratedM2StakerWithdrawals handles the WithdrawalMigrated event from the DelegationManager contract
//
// Since we have already counted M1 withdrawals due to processing events block-by-block, we need to handle not double subtracting.
// Assuming that M2 WithdrawalQueued events always result in a subtraction, if we encounter a migration event, we need
// to add the amount back to the shares to get the correct final state.
func (ss *StakerSharesModel) handleMigratedM2StakerWithdrawals(log *storage.TransactionLog) ([]*AccumulatedStateChange, error) {
	outputData, err := parseLogOutputForM2MigrationEvent(log.OutputData)
	if err != nil {
		return nil, err
	}
	query := `
		with migration as (
			select
				json_extract(tl.output_data, '$.nonce') as nonce,
				coalesce(json_extract(tl.output_data, '$.depositor'), json_extract(tl.output_data, '$.staker')) as staker
			from transaction_logs tl
			where
				tl.address = @strategyManagerAddress
				and tl.block_number <= @logBlockNumber
				and tl.event_name = 'WithdrawalQueued'
				and bytes_to_hex(json_extract(tl.output_data, '$.withdrawalRoot')) = @oldWithdrawalRoot
		),
		share_withdrawal_queued as (
			select
				tl.*,
				json_extract(tl.output_data, '$.nonce') as nonce,
				coalesce(json_extract(tl.output_data, '$.depositor'), json_extract(tl.output_data, '$.staker')) as staker
			from transaction_logs as tl
			where
				tl.address = @strategyManagerAddress
				and tl.event_name = 'ShareWithdrawalQueued'
		)
		select
			*
		from share_withdrawal_queued
		where
			nonce = (select nonce from migration)
			and staker = (select staker from migration)
	`
	logs := make([]storage.TransactionLog, 0)
	res := ss.DB.
		Raw(query,
			sql.Named("strategyManagerAddress", ss.globalConfig.GetContractsMapForChain().StrategyManager),
			sql.Named("logBlockNumber", log.BlockNumber),
			sql.Named("oldWithdrawalRoot", outputData.OldWithdrawalRootString),
		).
		Scan(&logs)

	if res.Error != nil {
		ss.logger.Sugar().Errorw("Failed to fetch share withdrawal queued logs", zap.Error(res.Error))
		return nil, res.Error
	}

	changes := make([]*AccumulatedStateChange, 0)
	for _, l := range logs {
		c, err := ss.handleStakerDepositEvent(&l)
		if err != nil {
			return nil, err
		}
		changes = append(changes, c)
	}

	return changes, nil
}

type m2WithdrawalOutputData struct {
	Withdrawal struct {
		Nonce      int           `json:"nonce"`
		Shares     []json.Number `json:"shares"`
		Staker     string        `json:"staker"`
		StartBlock uint64        `json:"startBlock"`
		Strategies []string      `json:"strategies"`
	} `json:"withdrawal"`
	WithdrawalRoot       []byte `json:"withdrawalRoot"`
	WithdrawalRootString string
}

func parseLogOutputForM2WithdrawalEvent(outputDataStr string) (*m2WithdrawalOutputData, error) {
	outputData := &m2WithdrawalOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}
	outputData.Withdrawal.Staker = strings.ToLower(outputData.Withdrawal.Staker)
	outputData.WithdrawalRootString = hex.EncodeToString(outputData.WithdrawalRoot)
	return outputData, err
}

// handleM2QueuedWithdrawal handles the WithdrawalQueued event from the DelegationManager contract for M2.
func (ss *StakerSharesModel) handleM2QueuedWithdrawal(log *storage.TransactionLog) ([]*AccumulatedStateChange, error) {
	outputData, err := parseLogOutputForM2WithdrawalEvent(log.OutputData)
	if err != nil {
		return nil, err
	}

	records := make([]*AccumulatedStateChange, 0)

	for i, strategy := range outputData.Withdrawal.Strategies {
		shares, success := numbers.NewBig257().SetString(outputData.Withdrawal.Shares[i].String(), 10)
		if !success {
			return nil, xerrors.Errorf("Failed to convert shares to big.Int: %s", outputData.Withdrawal.Shares[i])
		}
		r := &AccumulatedStateChange{
			Staker:      outputData.Withdrawal.Staker,
			Strategy:    strategy,
			Shares:      shares.Mul(shares, big.NewInt(-1)),
			BlockNumber: log.BlockNumber,
		}
		records = append(records, r)
	}
	return records, nil
}

type AccumulatedStateChanges struct {
	Changes []*AccumulatedStateChange
}

func (ss *StakerSharesModel) GetStateTransitions() types.StateTransitions {
	stateTransitions := make(types.StateTransitions)

	stateTransitions[0] = func(log *storage.TransactionLog) (interface{}, error) {
		parsedRecords := make([]*AccumulatedStateChange, 0)
		var err error

		contractAddresses := ss.globalConfig.GetContractsMapForChain()

		// Staker shares is a bit more complex and has 4 possible contract/event combinations
		// that we need to handle
		if log.Address == contractAddresses.StrategyManager && log.EventName == "Deposit" {
			record, err := ss.handleStakerDepositEvent(log)
			if err == nil {
				parsedRecords = append(parsedRecords, record)
			}
		} else if log.Address == contractAddresses.EigenpodManager && log.EventName == "PodSharesUpdated" {
			record, err := ss.handlePodSharesUpdatedEvent(log)
			if err == nil {
				parsedRecords = append(parsedRecords, record)
			}
		} else if log.Address == contractAddresses.StrategyManager && log.EventName == "ShareWithdrawalQueued" && log.TransactionHash != "0x62eb0d0865b2636c74ed146e2d161e39e42b09bac7f86b8905fc7a830935dc1e" {
			record, err := ss.handleM1StakerWithdrawals(log)
			if err == nil {
				parsedRecords = append(parsedRecords, record)
			}
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "WithdrawalQueued" {
			records, err := ss.handleM2QueuedWithdrawal(log)
			if err == nil && records != nil {
				parsedRecords = append(parsedRecords, records...)
			}
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "WithdrawalMigrated" {
			records, err := ss.handleMigratedM2StakerWithdrawals(log)
			if err == nil {
				parsedRecords = append(parsedRecords, records...)
			}
		} else {
			ss.logger.Sugar().Debugw("Got stakerShares event that we don't handle",
				zap.String("eventName", log.EventName),
				zap.String("address", log.Address),
			)
		}
		if err != nil {
			return nil, err
		}

		// Sanity check to make sure we've got an initialized accumulator map for the block
		if _, ok := ss.stateAccumulator[log.BlockNumber]; !ok {
			return nil, xerrors.Errorf("No state accumulator found for block %d", log.BlockNumber)
		}

		for _, parsedRecord := range parsedRecords {
			if parsedRecord == nil {
				continue
			}
			slotId := NewSlotID(parsedRecord.Staker, parsedRecord.Strategy)
			record, ok := ss.stateAccumulator[log.BlockNumber][slotId]
			if !ok {
				record = parsedRecord
				ss.stateAccumulator[log.BlockNumber][slotId] = record
			} else {
				record.Shares = record.Shares.Add(record.Shares, parsedRecord.Shares)
			}
		}

		return &AccumulatedStateChanges{Changes: parsedRecords}, nil
	}

	return stateTransitions
}

func (ss *StakerSharesModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := ss.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.DelegationManager: {
			"WithdrawalMigrated",
			"WithdrawalQueued",
		},
		contracts.StrategyManager: {
			"Deposit",
			"ShareWithdrawalQueued",
		},
		contracts.EigenpodManager: {
			"PodSharesUpdated",
		},
	}
}

func (ss *StakerSharesModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := ss.getContractAddressesForEnvironment()
	return ss.BaseEigenState.IsInterestingLog(addresses, log)
}

func (ss *StakerSharesModel) SetupStateForBlock(blockNumber uint64) error {
	ss.stateAccumulator[blockNumber] = make(map[types.SlotID]*AccumulatedStateChange)
	return nil
}

func (ss *StakerSharesModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(ss.stateAccumulator, blockNumber)
	return nil
}

func (ss *StakerSharesModel) HandleLog(log *storage.TransactionLog) (interface{}, error) {
	stateTransitions := ss.GetStateTransitions()
	return ss.BaseEigenState.HandleLog(stateTransitions, log)
}

// prepareState prepares the state for commit by adding the new state to the existing state.
func (ss *StakerSharesModel) prepareState(blockNumber uint64) ([]*AccumulatedStateChange, error) {
	preparedState := make([]*AccumulatedStateChange, 0)

	accumulatedState, ok := ss.stateAccumulator[blockNumber]
	if !ok {
		err := xerrors.Errorf("No accumulated state found for block %d", blockNumber)
		ss.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	slotIds := make([]types.SlotID, 0)
	for slotId := range accumulatedState {
		slotIds = append(slotIds, slotId)
	}

	// Find only the records from the previous block, that are modified in this block
	query := `
		with ranked_rows as (
			select
				staker,
				strategy,
				shares,
				block_number,
				ROW_NUMBER() OVER (PARTITION BY staker, strategy ORDER BY block_number desc) as rn
			from staker_shares
			where
				concat(staker, '_', strategy) in @slotIds
		)
		select
			rr.staker,
			rr.strategy,
			rr.shares,
			rr.block_number
		from ranked_rows as rr
		where rn = 1
	`
	existingRecords := make([]StakerShares, 0)
	res := ss.DB.Model(&StakerShares{}).
		Raw(query,
			sql.Named("slotIds", slotIds),
		).
		Scan(&existingRecords)

	if res.Error != nil {
		ss.logger.Sugar().Errorw("Failed to fetch staker_shares", zap.Error(res.Error))
		return nil, res.Error
	}

	// Map the existing records to a map for easier lookup
	mappedRecords := make(map[types.SlotID]StakerShares)
	for _, record := range existingRecords {
		slotId := NewSlotID(record.Staker, record.Strategy)
		mappedRecords[slotId] = record
	}

	// Loop over our new state changes.
	// If the record exists in the previous block, add the shares to the existing shares
	for slotId, newState := range accumulatedState {
		prepared := &AccumulatedStateChange{
			Staker:      newState.Staker,
			Strategy:    newState.Strategy,
			Shares:      newState.Shares,
			BlockNumber: blockNumber,
		}

		if existingRecord, ok := mappedRecords[slotId]; ok {
			existingShares, success := numbers.NewBig257().SetString(existingRecord.Shares, 10)
			if !success {
				ss.logger.Sugar().Errorw("Failed to convert existing shares to big.Int",
					zap.String("shares", existingRecord.Shares),
					zap.String("staker", existingRecord.Staker),
					zap.String("strategy", existingRecord.Strategy),
					zap.Uint64("blockNumber", blockNumber),
				)
				continue
			}
			prepared.Shares = existingShares.Add(existingShares, newState.Shares)
		}

		preparedState = append(preparedState, prepared)
	}
	return preparedState, nil
}

func (ss *StakerSharesModel) CommitFinalState(blockNumber uint64) error {
	records, err := ss.prepareState(blockNumber)
	if err != nil {
		return err
	}

	recordsToInsert := pkgUtils.Map(records, func(r *AccumulatedStateChange, i uint64) *StakerShares {
		return &StakerShares{
			Staker:      r.Staker,
			Strategy:    r.Strategy,
			Shares:      r.Shares.String(),
			BlockNumber: blockNumber,
		}
	})

	if len(recordsToInsert) > 0 {
		res := ss.DB.Model(&StakerShares{}).Clauses(clause.Returning{}).Create(&recordsToInsert)
		if res.Error != nil {
			ss.logger.Sugar().Errorw("Failed to create new operator_shares records", zap.Error(res.Error))
			return res.Error
		}
	}

	return nil
}

func (ss *StakerSharesModel) GenerateStateRoot(blockNumber uint64) (types.StateRoot, error) {
	diffs, err := ss.prepareState(blockNumber)
	if err != nil {
		return "", err
	}

	stateDiffs := make([]*base.StateDiff, 0)
	for _, diff := range diffs {
		stateDiffs = append(stateDiffs, &base.StateDiff{
			SlotID: NewSlotID(diff.Staker, diff.Strategy),
			Value:  diff.Shares.Bytes(),
		})
	}

	return ss.BaseEigenState.MerkleizeState(blockNumber, stateDiffs)
}

func (ss *StakerSharesModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return ss.BaseEigenState.DeleteState("staker_shares", startBlockNumber, endBlockNumber)
}
