package stakerShares

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/types/numbers"
	"math/big"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type AccumulatedStateChange struct {
	Staker      string
	Strategy    string
	Shares      *big.Int
	BlockNumber uint64
}

// Table staker_share_deltas
type StakerShareDeltas struct {
	Staker               string
	Strategy             string
	Shares               string
	StrategyIndex        uint64
	TransactionHash      string
	LogIndex             uint64
	BlockTime            time.Time
	BlockDate            string
	BlockNumber          uint64
	WithdrawalRootString string `gorm:"-"`
}

func NewSlotID(transactionHash string, logIndex uint64, staker string, strategy string, strategyIndex uint64) types.SlotID {
	return base.NewSlotIDWithSuffix(transactionHash, logIndex, fmt.Sprintf("%s_%s_%016x", staker, strategy, strategyIndex))
}

type StakerSharesModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[AccumulatedStateChange]
	DB               *gorm.DB
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates deltas for each block
	stateAccumulator map[uint64][]*StakerShareDeltas
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
		stateAccumulator: make(map[uint64][]*StakerShareDeltas),
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
		return nil, fmt.Errorf("No staker address found in event")
	}

	shares, success := numbers.NewBig257().SetString(outputData.Shares.String(), 10)
	if !success {
		return nil, fmt.Errorf("Failed to convert shares to big.Int: %s", outputData.Shares)
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
		return nil, fmt.Errorf("Failed to convert shares to big.Int: %s", sharesDelta)
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
		return nil, fmt.Errorf("No staker address found in event")
	}

	shares, success := numbers.NewBig257().SetString(outputData.Shares.String(), 10)
	if !success {
		return nil, fmt.Errorf("Failed to convert shares to big.Int: %s", outputData.Shares)
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
	NewWithdrawalRoot       []byte `json:"newWithdrawalRoot"`
	NewWithdrawalRootString string
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
	outputData.NewWithdrawalRootString = hex.EncodeToString(outputData.NewWithdrawalRoot)
	return outputData, err
}

// handleMigratedM2StakerWithdrawals handles the WithdrawalMigrated event from the DelegationManager contract
//
// Returns a list of M2 withdrawals that also correspond to an M1 withdrawal in order to not double count
func (ss *StakerSharesModel) handleMigratedM2StakerWithdrawals(log *storage.TransactionLog) ([]*StakerShareDeltas, error) {
	outputData, err := parseLogOutputForM2MigrationEvent(log.OutputData)
	if err != nil {
		return nil, err
	}
	// An M2 migration will have an oldWithdrawalRoot and a newWithdrawalRoot.
	// A `WithdrawalQueued` that was part of a migration will have a withdrawalRoot that matches the newWithdrawalRoot of the migration event.
	// We need to capture that value and remove the M2 withdrawal from the accumulator.
	//
	// In the case of a pure M2 withdrawal (not migrated), the withdrawalRoot will not match the newWithdrawalRoot.

	query := `
		with m2_withdrawal as (
			select
				*
			from transaction_logs as tl
			where
				tl.event_name = 'WithdrawalQueued'
				and tl.address = @delegationManagerAddress
				and lower((
				  SELECT lower(string_agg(lpad(to_hex(elem::int), 2, '0'), ''))
				  FROM jsonb_array_elements_text(tl.output_data ->'withdrawalRoot') AS elem
				)) = lower(@newWithdrawalRoot)
		)
		select * from m2_withdrawal
	`

	logs := make([]storage.TransactionLog, 0)
	res := ss.DB.
		Raw(query,
			sql.Named("delegationManagerAddress", ss.globalConfig.GetContractsMapForChain().DelegationManager),
			sql.Named("newWithdrawalRoot", outputData.NewWithdrawalRootString),
		).
		Scan(&logs)

	if res.Error != nil {
		ss.logger.Sugar().Errorw("Failed to fetch share withdrawal queued logs", zap.Error(res.Error))
		return nil, res.Error
	}

	changes := make([]*StakerShareDeltas, 0)
	for _, l := range logs {
		// The log is an M2 withdrawal, so parse it as such
		c, err := ss.handleM2QueuedWithdrawal(&l)
		if err != nil {
			return nil, err
		}
		if len(c) > 0 {
			changes = append(changes, c...)
		}
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
			return nil, fmt.Errorf("Failed to convert shares to big.Int: %s", outputData.Withdrawal.Shares[i])
		}
		r := &StakerShareDeltas{
			Staker:               outputData.Withdrawal.Staker,
			Strategy:             strategy,
			Shares:               shares.Mul(shares, big.NewInt(-1)).String(),
			StrategyIndex:        uint64(i),
			LogIndex:             log.LogIndex,
			TransactionHash:      log.TransactionHash,
			BlockNumber:          log.BlockNumber,
			WithdrawalRootString: outputData.WithdrawalRootString,
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

// handleSlashingWithdrawalQueued handles the WithdrawalQueued event from the DelegationManager contract for slashing
func (ss *StakerSharesModel) handleSlashingWithdrawalQueued(log *storage.TransactionLog) ([]*StakerShareDeltas, error) {
	// do the same thing as handleM2QueuedWithdrawal
	outputData, err := parseLogOutputForSlashingWithdrawalQueuedEvent(log.OutputData)
	if err != nil {
		return nil, err
	}

	records := make([]*StakerShareDeltas, 0)
	for i, strategy := range outputData.Withdrawal.Strategies {
		shares, success := numbers.NewBig257().SetString(outputData.SharesToWithdraw[i].String(), 10)
		if !success {
			return nil, fmt.Errorf("Failed to convert shares to big.Int: %s", outputData.SharesToWithdraw[i])
		}
		r := &StakerShareDeltas{
			Staker:               outputData.Withdrawal.Staker,
			Strategy:             strategy,
			Shares:               shares.Mul(shares, big.NewInt(-1)).String(),
			StrategyIndex:        uint64(i),
			LogIndex:             log.LogIndex,
			TransactionHash:      log.TransactionHash,
			BlockNumber:          log.BlockNumber,
			WithdrawalRootString: outputData.WithdrawalRootString,
		}
		records = append(records, r)
	}
	return records, nil
}

type AccumulatedStateChanges struct {
	Changes []*StakerShareDeltas
}

// GetStateTransitions returns a map of block numbers to state transitions and a list of block numbers
func (ss *StakerSharesModel) GetStateTransitions() (types.StateTransitions[*AccumulatedStateChanges], []uint64) {
	stateChanges := make(types.StateTransitions[*AccumulatedStateChanges])

	/**
	Order of StakerShare deposit and withdrawal events over time:

	- Deposit (strategy manager)
	- M1 ShareWithdrawalQueued (strategy manager)
	- M2 WithdrawalQueued (delegation manager)
	- M2 WithdrawalMigrated (delegation manager)
	- PodSharesUpdated (eigenpod manager)

	In the case of M2, M2 WithdrawalQueued handles BOTH standard M2 withdrawals and was paired with M2 WithdrawalMigrated
	for the cases where M1 withdrawals were migrated to M2.

	M1 to M2 Migrations happened in the order of:
	1. WithdrawalQueued
	2. WithdrawalMigrated

	When we come across an M2 WithdrawalMigrated event, we need to check and see if it has a corresponding M2 WithdrawalQueued event
	and then remove the WithdrawalQueued event from the accumulator to prevent double counting.

	This is done by comparing:

	M2.WithdrawalQueued.WithdrawalRoot == M2.WithdrawalMigrated.NewWithdrawalRoot
	*/
	stateChanges[0] = func(log *storage.TransactionLog) (*AccumulatedStateChanges, error) {
		deltaRecords := make([]*StakerShareDeltas, 0)
		var err error

		contractAddresses := ss.globalConfig.GetContractsMapForChain()

		// Staker shares is a bit more complex and has 4 possible contract/event combinations
		// that we need to handle
		if log.Address == contractAddresses.StrategyManager && log.EventName == "Deposit" {
			record, err := ss.handleStakerDepositEvent(log)
			if err == nil {
				deltaRecords = append(deltaRecords, record)
			}
		} else if log.Address == contractAddresses.EigenpodManager && log.EventName == "PodSharesUpdated" {
			record, err := ss.handlePodSharesUpdatedEvent(log)
			if err == nil {
				deltaRecords = append(deltaRecords, record)
			}
		} else if log.Address == contractAddresses.StrategyManager && log.EventName == "ShareWithdrawalQueued" && log.TransactionHash != "0x62eb0d0865b2636c74ed146e2d161e39e42b09bac7f86b8905fc7a830935dc1e" {
			record, err := ss.handleM1StakerWithdrawals(log)
			if err == nil {
				deltaRecords = append(deltaRecords, record)
			}
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "WithdrawalQueued" {
			records, err := ss.handleM2QueuedWithdrawal(log)
			if err == nil && records != nil {
				deltaRecords = append(deltaRecords, records...)
			}
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "WithdrawalMigrated" {
			migratedM2WithdrawalsToRemove, err := ss.handleMigratedM2StakerWithdrawals(log)
			if err == nil {
				// Iterate over the list of M2 withdrawals to remove to prevent double counting
				for _, record := range migratedM2WithdrawalsToRemove {

					// The massive caveat with this is that it assumes that the M2 withdrawal and corresponding
					// migration events are processed in the same block, which was in fact the case.
					//
					// The M2 WithdrawalQueued event will come first
					// then the M2 WithdrawalMigrated event will come second
					filteredDeltas := make([]*StakerShareDeltas, 0)
					for _, delta := range ss.stateAccumulator[log.BlockNumber] {
						if delta.WithdrawalRootString != record.WithdrawalRootString {
							filteredDeltas = append(filteredDeltas, delta)
						}
					}
					ss.stateAccumulator[log.BlockNumber] = filteredDeltas
				}
			}
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "SlashingWithdrawalQueued" {
			records, err := ss.handleSlashingWithdrawalQueued(log)
			if err == nil && records != nil {
				deltaRecords = append(deltaRecords, records...)
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

		ss.stateAccumulator[log.BlockNumber] = append(ss.stateAccumulator[log.BlockNumber], deltaRecords...)

		return &AccumulatedStateChanges{Changes: ss.stateAccumulator[log.BlockNumber]}, nil
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
	ss.stateAccumulator[blockNumber] = make([]*StakerShareDeltas, 0)
	return nil
}

func (ss *StakerSharesModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(ss.stateAccumulator, blockNumber)
	return nil
}

func (ss *StakerSharesModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := ss.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			ss.logger.Sugar().Debugw("Handling state change",
				zap.Uint64("blockNumber", log.BlockNumber),
				zap.String("eventName", log.EventName),
				zap.String("address", log.Address),
			)

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
func (ss *StakerSharesModel) prepareState(blockNumber uint64) ([]*StakerShareDeltas, error) {
	records, ok := ss.stateAccumulator[blockNumber]
	if !ok {
		msg := "delta accumulator was not initialized"
		ss.logger.Sugar().Errorw(msg, zap.Uint64("blockNumber", blockNumber))
		return nil, errors.New(msg)
	}
	return records, nil
}

func (ss *StakerSharesModel) writeDeltaRecords(blockNumber uint64) error {
	records, ok := ss.stateAccumulator[blockNumber]
	if !ok {
		msg := "accumulator was not initialized"
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
	if err := ss.writeDeltaRecords(blockNumber); err != nil {
		return err
	}

	return nil
}

func (ss *StakerSharesModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	deltas, err := ss.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs := ss.sortValuesForMerkleTree(deltas)

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := ss.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		ss.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (ss *StakerSharesModel) sortValuesForMerkleTree(diffs []*StakerShareDeltas) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, diff := range diffs {
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: NewSlotID(diff.TransactionHash, diff.LogIndex, diff.Staker, diff.Strategy, diff.StrategyIndex),
			Value:  []byte(diff.Shares),
		})
	}
	slices.SortFunc(inputs, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})

	return inputs
}

func (ss *StakerSharesModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return ss.BaseEigenState.DeleteState("staker_share_deltas", startBlockNumber, endBlockNumber, ss.DB)
}
