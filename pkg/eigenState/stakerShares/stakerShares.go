package stakerShares

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Layr-Labs/go-sidecar/pkg/storage"
	"github.com/Layr-Labs/go-sidecar/pkg/types/numbers"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/types"
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

type StakerSharesDiff struct {
	Staker      string
	Strategy    string
	Shares      *big.Int
	BlockNumber uint64
	IsNew       bool
}

// Table staker_share_deltas
type StakerShareDeltas struct {
	Staker          string
	Strategy        string
	Shares          string
	StrategyIndex   uint64
	TransactionHash string
	LogIndex        uint64
	BlockTime       time.Time
	BlockDate       string
	BlockNumber     uint64
}

func NewSlotID(staker string, strategy string) types.SlotID {
	return types.SlotID(fmt.Sprintf("%s_%s", staker, strategy))
}

type StakerSharesModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[AccumulatedStateChange]
	DB               *gorm.DB
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*AccumulatedStateChange

	deltaAccumulator map[uint64][]*StakerShareDeltas
}

func NewStakerSharesModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*StakerSharesModel, error) {
	model := &StakerSharesModel{
		BaseEigenState:   base.BaseEigenState{},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*AccumulatedStateChange),
		deltaAccumulator: make(map[uint64][]*StakerShareDeltas),
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

func (ss *StakerSharesModel) handleStakerDepositEvent(log *storage.TransactionLog) (*StakerShareDeltas, error) {
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

	return &StakerShareDeltas{
		Staker:          stakerAddress,
		Strategy:        outputData.Strategy,
		Shares:          shares.String(),
		StrategyIndex:   uint64(0),
		LogIndex:        log.LogIndex,
		TransactionHash: log.TransactionHash,
		BlockNumber:     log.BlockNumber,
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

func (ss *StakerSharesModel) handlePodSharesUpdatedEvent(log *storage.TransactionLog) (*StakerShareDeltas, error) {
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

	return &StakerShareDeltas{
		Staker:          staker,
		Strategy:        "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0",
		Shares:          sharesDelta.String(),
		StrategyIndex:   uint64(0),
		LogIndex:        log.LogIndex,
		TransactionHash: log.TransactionHash,
		BlockNumber:     log.BlockNumber,
	}, nil
}

func (ss *StakerSharesModel) handleM1StakerWithdrawals(log *storage.TransactionLog) (*StakerShareDeltas, error) {
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

	return &StakerShareDeltas{
		Staker:          stakerAddress,
		Strategy:        outputData.Strategy,
		Shares:          shares.Mul(shares, big.NewInt(-1)).String(),
		StrategyIndex:   uint64(0),
		LogIndex:        log.LogIndex,
		TransactionHash: log.TransactionHash,
		BlockNumber:     log.BlockNumber,
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
func (ss *StakerSharesModel) handleMigratedM2StakerWithdrawals(log *storage.TransactionLog) ([]*StakerShareDeltas, error) {
	outputData, err := parseLogOutputForM2MigrationEvent(log.OutputData)
	if err != nil {
		return nil, err
	}
	query := `
		WITH migration AS (
			SELECT
				(tl.output_data ->> 'nonce') AS nonce,
				lower(coalesce(tl.output_data ->> 'depositor', tl.output_data ->> 'staker')) AS staker
			FROM transaction_logs tl
			WHERE
				tl.address = @strategyManagerAddress
				AND tl.block_number <= @logBlockNumber
				AND tl.event_name = 'WithdrawalQueued'
				AND (
					SELECT lower(string_agg(lpad(to_hex(elem::integer), 2, '0'), ''))
					FROM jsonb_array_elements_text(tl.output_data->'withdrawalRoot') AS elem
				) = @oldWithdrawalRoot
		),
		share_withdrawal_queued AS (
			SELECT
				tl.*,
				(tl.output_data ->> 'nonce') AS nonce,
				lower(coalesce(tl.output_data ->> 'depositor', tl.output_data ->> 'staker')) AS staker
			FROM transaction_logs AS tl
			WHERE
				tl.address = @strategyManagerAddress
				AND tl.event_name = 'ShareWithdrawalQueued'
		)
		SELECT
			*
		FROM share_withdrawal_queued
		WHERE
			nonce = (SELECT nonce FROM migration)
			AND staker = (SELECT staker FROM migration)
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

	changes := make([]*StakerShareDeltas, 0)
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
func (ss *StakerSharesModel) handleM2QueuedWithdrawal(log *storage.TransactionLog) ([]*StakerShareDeltas, error) {
	outputData, err := parseLogOutputForM2WithdrawalEvent(log.OutputData)
	if err != nil {
		return nil, err
	}

	records := make([]*StakerShareDeltas, 0)

	for i, strategy := range outputData.Withdrawal.Strategies {
		shares, success := numbers.NewBig257().SetString(outputData.Withdrawal.Shares[i].String(), 10)
		if !success {
			return nil, xerrors.Errorf("Failed to convert shares to big.Int: %s", outputData.Withdrawal.Shares[i])
		}
		r := &StakerShareDeltas{
			Staker:          outputData.Withdrawal.Staker,
			Strategy:        strategy,
			Shares:          shares.Mul(shares, big.NewInt(-1)).String(),
			StrategyIndex:   uint64(i),
			LogIndex:        log.LogIndex,
			TransactionHash: log.TransactionHash,
			BlockNumber:     log.BlockNumber,
		}
		records = append(records, r)
	}
	return records, nil
}

type slashingWithdrawalQueuedOutputData struct {
	Withdrawal struct {
		Nonce        int           `json:"nonce"`
		ScaledShares []json.Number `json:"scaledShares"`
		Staker       string        `json:"staker"`
		StartBlock   uint64        `json:"startBlock"`
		Strategies   []string      `json:"strategies"`
	} `json:"withdrawal"`
	WithdrawalRoot       []byte        `json:"withdrawalRoot"`
	SharesToWithdraw     []json.Number `json:"sharesToWithdraw"`
	WithdrawalRootString string
}

func parseLogOutputForSlashingWithdrawalQueuedEvent(outputDataStr string) (*slashingWithdrawalQueuedOutputData, error) {
	outputData := &slashingWithdrawalQueuedOutputData{}
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
func (ss *StakerSharesModel) handleSlashingQueuedWithdrawal(log *storage.TransactionLog) ([]*AccumulatedStateChange, error) {
	outputData, err := parseLogOutputForSlashingWithdrawalQueuedEvent(log.OutputData)
	if err != nil {
		return nil, err
	}

	records := make([]*AccumulatedStateChange, 0)

	for i, strategy := range outputData.Withdrawal.Strategies {
		shares, success := numbers.NewBig257().SetString(outputData.SharesToWithdraw[i].String(), 10)
		if !success {
			return nil, xerrors.Errorf("Failed to convert shares to big.Int: %s", outputData.SharesToWithdraw[i])
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

func shareDeltaToAccumulatedStateChange(deltaRecord *StakerShareDeltas) *AccumulatedStateChange {
	shares, _ := numbers.NewBig257().SetString(deltaRecord.Shares, 10)
	return &AccumulatedStateChange{
		Staker:      deltaRecord.Staker,
		Strategy:    deltaRecord.Strategy,
		Shares:      shares,
		BlockNumber: deltaRecord.BlockNumber,
	}
}

func (ss *StakerSharesModel) GetStateTransitions() (types.StateTransitions[AccumulatedStateChanges], []uint64) {
	stateChanges := make(types.StateTransitions[AccumulatedStateChanges])

	stateChanges[0] = func(log *storage.TransactionLog) (*AccumulatedStateChanges, error) {
		deltaRecords := make([]*StakerShareDeltas, 0)
		var parsedRecords []*AccumulatedStateChange
		var err error

		contractAddresses := ss.globalConfig.GetContractsMapForChain()

		// Staker shares is a bit more complex and has 4 possible contract/event combinations
		// that we need to handle
		if log.Address == contractAddresses.StrategyManager && log.EventName == "Deposit" {
			record, err := ss.handleStakerDepositEvent(log)
			if err == nil {
				deltaRecords = append(deltaRecords, record)
				parsedRecords = append(parsedRecords, shareDeltaToAccumulatedStateChange(record))
			}
		} else if log.Address == contractAddresses.EigenpodManager && log.EventName == "PodSharesUpdated" {
			record, err := ss.handlePodSharesUpdatedEvent(log)
			if err == nil {
				deltaRecords = append(deltaRecords, record)
				parsedRecords = append(parsedRecords, shareDeltaToAccumulatedStateChange(record))
			}
		} else if log.Address == contractAddresses.StrategyManager && log.EventName == "ShareWithdrawalQueued" && log.TransactionHash != "0x62eb0d0865b2636c74ed146e2d161e39e42b09bac7f86b8905fc7a830935dc1e" {
			record, err := ss.handleM1StakerWithdrawals(log)
			if err == nil {
				deltaRecords = append(deltaRecords, record)
				parsedRecords = append(parsedRecords, shareDeltaToAccumulatedStateChange(record))
			}
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "WithdrawalQueued" {
			records, err := ss.handleM2QueuedWithdrawal(log)
			if err == nil && records != nil {
				deltaRecords = append(deltaRecords, records...)
				for _, record := range records {
					parsedRecords = append(parsedRecords, shareDeltaToAccumulatedStateChange(record))
				}
			}
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "WithdrawalMigrated" {
			records, err := ss.handleMigratedM2StakerWithdrawals(log)
			if err == nil {
				// NOTE: we DONT add these to the delta table because they've already been handled

				for _, record := range records {
					parsedRecords = append(parsedRecords, shareDeltaToAccumulatedStateChange(record))
				}
			}
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "SlashingWithdrawalQueued" {
			records, err := ss.handleSlashingQueuedWithdrawal(log)
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
		if deltaRecords == nil {
			return nil, nil
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
		ss.deltaAccumulator[log.BlockNumber] = append(ss.deltaAccumulator[log.BlockNumber], deltaRecords...)

		return &AccumulatedStateChanges{Changes: parsedRecords}, nil
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

func (ss *StakerSharesModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := ss.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.DelegationManager: {
			"WithdrawalMigrated",
			"WithdrawalQueued",
			"SlashingWithdrawalQueued",
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
	ss.deltaAccumulator[blockNumber] = make([]*StakerShareDeltas, 0)
	return nil
}

func (ss *StakerSharesModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(ss.stateAccumulator, blockNumber)
	delete(ss.deltaAccumulator, blockNumber)
	return nil
}

func (ss *StakerSharesModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := ss.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			ss.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", blockNumber))

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
func (ss *StakerSharesModel) prepareState(blockNumber uint64) ([]*StakerSharesDiff, error) {
	preparedState := make([]*StakerSharesDiff, 0)

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
		prepared := &StakerSharesDiff{
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

func (ss *StakerSharesModel) writeDeltaRecordsToDeltaTable(blockNumber uint64) error {
	records, ok := ss.deltaAccumulator[blockNumber]
	if !ok {
		msg := "delta accumulator was not initialized"
		ss.logger.Sugar().Errorw(msg, zap.Uint64("blockNumber", blockNumber))
		return errors.New(msg)
	}

	if len(records) == 0 {
		return nil
	}
	var block storage.Block
	res := ss.DB.Model(&storage.Block{}).Where("number = ?", blockNumber).First(&block)
	if res.Error != nil {
		ss.logger.Sugar().Errorw("Failed to fetch block", zap.Error(res.Error))
		return res.Error
	}

	for _, r := range records {
		r.BlockTime = block.BlockTime
		r.BlockDate = block.BlockTime.Format(time.DateOnly)
	}

	res = ss.DB.Model(&StakerShareDeltas{}).Clauses(clause.Returning{}).Create(&records)
	if res.Error != nil {
		ss.logger.Sugar().Errorw("Failed to insert delta records", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

func (ss *StakerSharesModel) CommitFinalState(blockNumber uint64) error {
	records, err := ss.prepareState(blockNumber)
	if err != nil {
		return err
	}

	recordsToInsert := pkgUtils.Map(records, func(r *StakerSharesDiff, i uint64) *StakerShares {
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

	if err = ss.writeDeltaRecordsToDeltaTable(blockNumber); err != nil {
		return err
	}

	return nil
}

func (ss *StakerSharesModel) GenerateStateRoot(blockNumber uint64) (types.StateRoot, error) {
	diffs, err := ss.prepareState(blockNumber)
	if err != nil {
		return "", err
	}

	inputs := ss.sortValuesForMerkleTree(diffs)

	fullTree, err := ss.MerkleizeState(blockNumber, inputs)
	if err != nil {
		return "", err
	}
	return types.StateRoot(pkgUtils.ConvertBytesToString(fullTree.Root())), nil
}

func (ss *StakerSharesModel) sortValuesForMerkleTree(diffs []*StakerSharesDiff) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, diff := range diffs {
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: NewSlotID(diff.Staker, diff.Strategy),
			Value:  diff.Shares.Bytes(),
		})
	}
	slices.SortFunc(inputs, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})

	return inputs
}

func (ss *StakerSharesModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return ss.BaseEigenState.DeleteState("staker_shares", startBlockNumber, endBlockNumber, ss.DB)
}
