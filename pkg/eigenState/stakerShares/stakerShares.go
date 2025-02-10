package stakerShares

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/types/numbers"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SlashDiff struct {
	// the entity that was slashed, the operator in the case of an EigenLayer slashing,
	// the pod owner in the case of a beacon chain slashing. Note that an EigenLayer slashing
	// still slashes all of the stakers that are delegated to the operator.
	SlashedEntity string
	// whether the slashing was done by the beacon chain or an EigenLayer AVS
	BeaconChain     bool
	Strategy        string
	WadSlashed      *big.Int
	TransactionHash string
	LogIndex        uint64
	BlockTime       time.Time
	BlockDate       string
	BlockNumber     uint64
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
	DB           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	// Accumulates deltas for each block
	committedState        map[uint64][]*StakerShareDeltas
	shareDeltaAccumulator map[uint64][]*StakerShareDeltas
	slashingAccumulator   map[uint64][]*SlashDiff
}

func NewStakerSharesModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*StakerSharesModel, error) {
	model := &StakerSharesModel{
		BaseEigenState:        base.BaseEigenState{},
		DB:                    grm,
		logger:                logger,
		globalConfig:          globalConfig,
		committedState:        make(map[uint64][]*StakerShareDeltas),
		shareDeltaAccumulator: make(map[uint64][]*StakerShareDeltas),
		slashingAccumulator:   make(map[uint64][]*SlashDiff),
	}

	esm.RegisterState(model, 3)
	return model, nil
}

const StakerSharesModelName = "StakerSharesModel"

func (ss *StakerSharesModel) GetModelName() string {
	return StakerSharesModelName
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

type operatorSlashedOutputData struct {
	Operator   string        `json:"operator"`
	Strategies []string      `json:"strategies"`
	WadSlashed []json.Number `json:"wadSlashed"`
}

func parseLogOutputForOperatorSlashedEvent(outputDataStr string) (*operatorSlashedOutputData, error) {
	outputData := &operatorSlashedOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (ss *StakerSharesModel) handleOperatorSlashedEvent(log *storage.TransactionLog) ([]*SlashDiff, error) {
	outputData, err := parseLogOutputForOperatorSlashedEvent(log.OutputData)
	if err != nil {
		return nil, err
	}

	stateDiffs := make([]*SlashDiff, 0)

	for i, strategy := range outputData.Strategies {
		wadSlashed, success := numbers.NewBig257().SetString(outputData.WadSlashed[i].String(), 10)
		if !success {
			return nil, fmt.Errorf("Failed to convert wadSlashed to big.Int: %s", outputData.WadSlashed[i])
		}
		stateDiffs = append(stateDiffs, &SlashDiff{
			SlashedEntity:   outputData.Operator,
			BeaconChain:     false,
			Strategy:        strategy,
			WadSlashed:      wadSlashed,
			TransactionHash: log.TransactionHash,
			LogIndex:        log.LogIndex,
			BlockNumber:     log.BlockNumber,
		})
	}

	return stateDiffs, nil
}

// event BeaconChainSlashingFactorDecreased(
//
//	address staker, uint64 prevBeaconChainSlashingFactor, uint64 newBeaconChainSlashingFactor
//
// );
type beaconChainSlashingFactorDecreasedOutputData struct {
	Staker                        string `json:"staker"`
	PrevBeaconChainSlashingFactor uint64 `json:"prevBeaconChainSlashingFactor"`
	NewBeaconChainSlashingFactor  uint64 `json:"newBeaconChainSlashingFactor"`
}

func parseLogOutputForBeaconChainSlashingFactorDecreasedEvent(outputDataStr string) (*beaconChainSlashingFactorDecreasedOutputData, error) {
	outputData := &beaconChainSlashingFactorDecreasedOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (ss *StakerSharesModel) handleBeaconChainSlashingFactorDecreasedEvent(log *storage.TransactionLog) (*SlashDiff, error) {
	outputData, err := parseLogOutputForBeaconChainSlashingFactorDecreasedEvent(log.OutputData)
	if err != nil {
		return nil, err
	}

	wadSlashed := big.NewInt(1e18)
	wadSlashed = wadSlashed.Mul(wadSlashed, new(big.Int).SetUint64(outputData.NewBeaconChainSlashingFactor))
	wadSlashed = wadSlashed.Div(wadSlashed, new(big.Int).SetUint64(outputData.PrevBeaconChainSlashingFactor))
	wadSlashed = wadSlashed.Sub(big.NewInt(1e18), wadSlashed)

	return &SlashDiff{
		SlashedEntity:   outputData.Staker,
		BeaconChain:     true,
		Strategy:        "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0",
		WadSlashed:      wadSlashed,
		TransactionHash: log.TransactionHash,
		LogIndex:        log.LogIndex,
		BlockNumber:     log.BlockNumber,
	}, nil
}

type AccumulatedStateChanges struct {
	ShareDeltas []*StakerShareDeltas
	SlashDiffs  []*SlashDiff
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
		shareDeltaRecords := make([]*StakerShareDeltas, 0)
		slashDiffs := make([]*SlashDiff, 0)
		contractAddresses := ss.globalConfig.GetContractsMapForChain()

		// Staker shares is a bit more complex and has 4 possible contract/event combinations
		// that we need to handle
		if log.Address == contractAddresses.StrategyManager && log.EventName == "Deposit" {
			record, err := ss.handleStakerDepositEvent(log)
			if err != nil {
				return nil, err
			}
			shareDeltaRecords = append(shareDeltaRecords, record)
		} else if log.Address == contractAddresses.EigenpodManager && log.EventName == "PodSharesUpdated" {
			record, err := ss.handlePodSharesUpdatedEvent(log)
			if err != nil {
				return nil, err
			}
			shareDeltaRecords = append(shareDeltaRecords, record)
		} else if log.Address == contractAddresses.StrategyManager && log.EventName == "ShareWithdrawalQueued" {
			record, err := ss.handleM1StakerWithdrawals(log)
			if err != nil {
				return nil, err
			}
			shareDeltaRecords = append(shareDeltaRecords, record)
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "WithdrawalQueued" {
			records, err := ss.handleM2QueuedWithdrawal(log)
			if err != nil {
				return nil, err
			}
			shareDeltaRecords = append(shareDeltaRecords, records...)
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "WithdrawalMigrated" {
			migratedM2WithdrawalsToRemove, err := ss.handleMigratedM2StakerWithdrawals(log)
			if err != nil {
				return nil, err
			}
			// Iterate over the list of M2 withdrawals to remove to prevent double counting
			for _, record := range migratedM2WithdrawalsToRemove {

				// The massive caveat with this is that it assumes that the M2 withdrawal and corresponding
				// migration events are processed in the same block, which was in fact the case.
				//
				// The M2 WithdrawalQueued event will come first
				// then the M2 WithdrawalMigrated event will come second
				filteredDeltas := make([]*StakerShareDeltas, 0)
				for _, delta := range ss.shareDeltaAccumulator[log.BlockNumber] {
					if delta.WithdrawalRootString != record.WithdrawalRootString {
						filteredDeltas = append(filteredDeltas, delta)
					}
				}
				ss.shareDeltaAccumulator[log.BlockNumber] = filteredDeltas
			}
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "SlashingWithdrawalQueued" {
			records, err := ss.handleSlashingWithdrawalQueued(log)
			if err != nil {
				return nil, err
			}
			shareDeltaRecords = append(shareDeltaRecords, records...)
		} else if log.Address == contractAddresses.AllocationManager && log.EventName == "OperatorSlashed" {
			records, err := ss.handleOperatorSlashedEvent(log)
			if err != nil {
				return nil, err
			}
			slashDiffs = append(slashDiffs, records...)
		} else if log.Address == contractAddresses.EigenpodManager && log.EventName == "BeaconChainSlashingFactorDecreased" {
			record, err := ss.handleBeaconChainSlashingFactorDecreasedEvent(log)
			if err != nil {
				return nil, err
			}
			slashDiffs = append(slashDiffs, record)
		} else {
			ss.logger.Sugar().Debugw("Got stakerShares event that we don't handle",
				zap.String("eventName", log.EventName),
				zap.String("address", log.Address),
			)
		}

		ss.shareDeltaAccumulator[log.BlockNumber] = append(ss.shareDeltaAccumulator[log.BlockNumber], shareDeltaRecords...)
		ss.slashingAccumulator[log.BlockNumber] = append(ss.slashingAccumulator[log.BlockNumber], slashDiffs...)

		return &AccumulatedStateChanges{ShareDeltas: ss.shareDeltaAccumulator[log.BlockNumber], SlashDiffs: ss.slashingAccumulator[log.BlockNumber]}, nil
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
			"BeaconChainSlashingFactorDecreased",
		},
		contracts.AllocationManager: {
			"OperatorSlashed",
		},
	}
}

func (ss *StakerSharesModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := ss.getContractAddressesForEnvironment()
	return ss.BaseEigenState.IsInterestingLog(addresses, log)
}

func (ss *StakerSharesModel) SetupStateForBlock(blockNumber uint64) error {
	ss.committedState[blockNumber] = make([]*StakerShareDeltas, 0)
	ss.shareDeltaAccumulator[blockNumber] = make([]*StakerShareDeltas, 0)
	ss.slashingAccumulator[blockNumber] = make([]*SlashDiff, 0)
	return nil
}

func (ss *StakerSharesModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(ss.committedState, blockNumber)
	delete(ss.shareDeltaAccumulator, blockNumber)
	delete(ss.slashingAccumulator, blockNumber)
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

type StakerShares struct {
	Staker string
	Shares string
}

// GetDelegatedStakerSharesAtTimeOfSlashing returns the shares of the stakers that were delegated to the operator at the time of slashing for the strategy being slashed
// it will return 0 shares for stakers were delegated, but have no shares in the strategy being slashed
func (ss *StakerSharesModel) GetDelegatedStakerSharesAtTimeOfSlashing(slashDiff *SlashDiff) (*[]StakerShares, error) {
	query := `
		with ranked_staker_delegations as (
			select
				staker,
				operator,
				delegated,
				ROW_NUMBER() OVER (PARTITION BY staker ORDER BY block_number desc, log_index desc) as rn
			from staker_delegation_changes
			where
				block_number <= @blockNumber
		),
		delegated_stakers as (
			select
				lsd.staker
			from ranked_staker_delegations as lsd
			where
				lsd.operator = @operator and
				lsd.delegated = true and
				lsd.rn = 1
		)
		select
			ds.staker as staker,
			COALESCE(sum(ssd.shares), 0) as shares
		from
			delegated_stakers as ds
		left join
			staker_share_deltas as ssd
			on ssd.staker = ds.staker
			and ssd.strategy = @strategy
			and ssd.block_number <= @blockNumber
		group by
			ds.staker
	`
	stakerShares := make([]StakerShares, 0)

	// get the staker shares for the stakers who were delegated to the operator
	// and update the shares with the new max magnitude
	res := ss.DB.Raw(query,
		sql.Named("blockNumber", slashDiff.BlockNumber),
		sql.Named("logIndex", slashDiff.LogIndex),
		sql.Named("operator", slashDiff.SlashedEntity),
		sql.Named("strategy", slashDiff.Strategy),
	).Scan(&stakerShares)
	if res.Error != nil {
		ss.logger.Sugar().Errorw("Failed to fetch staker_shares", zap.Error(res.Error))
		return nil, res.Error
	}

	// log all the staker shares
	for _, stakerShare := range stakerShares {
		ss.logger.Sugar().Debugw("Staker shares",
			zap.String("staker", stakerShare.Staker),
			zap.String("strategy", slashDiff.Strategy),
			zap.String("shares", stakerShare.Shares),
		)
	}

	return &stakerShares, nil
}

func (ss *StakerSharesModel) GetStakerSharesFromDB(staker string, strategy string) (*[]StakerShares, error) {
	query := `
        select
            staker,
            sum(shares) as shares
        from staker_share_deltas
        where
            staker = @staker
            and strategy = @strategy
        group by
            staker,
            strategy
    `
	// there should only be one record for the pair
	stakerShares := make([]StakerShares, 0)
	res := ss.DB.Raw(query,
		sql.Named("staker", staker),
		sql.Named("strategy", strategy),
	).Scan(&stakerShares)
	if res.Error != nil {
		ss.logger.Sugar().Errorw("Failed to fetch staker_shares", zap.Error(res.Error))
		return nil, res.Error
	}

	// if there are no shares, return a record with 0 shares
	if len(stakerShares) == 0 {
		stakerShares = append(stakerShares, StakerShares{
			Staker: staker,
			Shares: "0",
		})
	}

	return &stakerShares, nil
}

// prepareState prepares the state for commit by adding the new state to the existing state.
func (ss *StakerSharesModel) prepareState(blockNumber uint64) ([]*StakerShareDeltas, error) {
	_, ok := ss.shareDeltaAccumulator[blockNumber]
	if !ok {
		msg := "delta accumulator was not initialized"
		ss.logger.Sugar().Errorw(msg, zap.Uint64("blockNumber", blockNumber))
		return nil, errors.New(msg)
	}
	slashes, ok := ss.slashingAccumulator[blockNumber]
	if !ok {
		msg := "slashing accumulator was not initialized"
		ss.logger.Sugar().Errorw(msg, zap.Uint64("blockNumber", blockNumber))
		return nil, errors.New(msg)
	}

	records := make([]*StakerShareDeltas, 0)

	netDeltas := make(map[string]*big.Int)

	shareDeltaIndex := 0
	slashingIndex := 0
	for shareDeltaIndex < len(ss.shareDeltaAccumulator[blockNumber]) || slashingIndex < len(ss.slashingAccumulator[blockNumber]) {
		// initialize to max logIndex so we can compare
		shareDelta := &StakerShareDeltas{LogIndex: math.MaxUint64}
		slashDiff := &SlashDiff{LogIndex: math.MaxUint64}

		// load the accumulators if index exists
		if shareDeltaIndex < len(ss.shareDeltaAccumulator[blockNumber]) {
			shareDelta = ss.shareDeltaAccumulator[blockNumber][shareDeltaIndex]
		}

		if slashingIndex < len(ss.slashingAccumulator[blockNumber]) {
			slashDiff = ss.slashingAccumulator[blockNumber][slashingIndex]
		}

		if shareDelta.LogIndex < slashDiff.LogIndex {
			key := fmt.Sprintf("%s-%s", shareDelta.Staker, shareDelta.Strategy)

			ss.logger.Sugar().Debugw("regular share delta",
				zap.String("staker", shareDelta.Staker),
				zap.String("strategy", shareDelta.Strategy),
				zap.String("shares", shareDelta.Shares),
			)

			// apply the shareDelta
			if _, ok := netDeltas[key]; !ok {
				netDeltas[key] = big.NewInt(0)
			}
			shares, success := numbers.NewBig257().SetString(shareDelta.Shares, 10)
			if !success {
				return nil, fmt.Errorf("failed to convert shares to big.Int: %s", shareDelta.Shares)
			}
			netDeltas[key] = netDeltas[key].Add(netDeltas[key], shares)
			records = append(records, shareDelta)

			shareDeltaIndex++
		} else {

			ss.logger.Sugar().Debugw("Slashing",
				zap.String("slashedEntity", slashDiff.SlashedEntity),
				zap.Bool("beaconChain", slashDiff.BeaconChain),
				zap.String("strategy", slashDiff.Strategy),
				zap.String("wadSlashed", slashDiff.WadSlashed.String()),
				zap.String("transactionHash", slashDiff.TransactionHash),
				zap.Uint64("logIndex", slashDiff.LogIndex),
			)

			var stakerShares *[]StakerShares
			var err error
			if !slashDiff.BeaconChain {
				stakerShares, err = ss.GetDelegatedStakerSharesAtTimeOfSlashing(slashDiff)
			} else {
				stakerShares, err = ss.GetStakerSharesFromDB(slashDiff.SlashedEntity, slashDiff.Strategy)
			}
			if err != nil {
				return nil, err
			}

			for _, stakerShare := range *stakerShares {
				// loop through every delegated staker
				// check if they have previous deposits, withdrawals, or slashes in the current block that need to be taken into account
				key := fmt.Sprintf("%s-%s", stakerShare.Staker, slashDiff.Strategy)
				_, relevantDeltasInCurrentBlock := netDeltas[key]
				// if they have shares in the strategy being slashed before the current block
				// or they have deltas for the strategy being slashed in the current block
				if !strings.EqualFold(stakerShare.Shares, "0") || relevantDeltasInCurrentBlock {
					sharesBeforeSlash, success := numbers.NewBig257().SetString(stakerShare.Shares, 10)
					if !success {
						return nil, fmt.Errorf("failed to convert shares to big.Int: %s", stakerShare.Shares)
					}

					// add the net delta in this block
					if !relevantDeltasInCurrentBlock {
						netDeltas[key] = big.NewInt(0)
					}
					sharesBeforeSlash = sharesBeforeSlash.Add(sharesBeforeSlash, netDeltas[key])

					// add a delta for the slashing
					sharesSlashed := new(big.Int).Div(new(big.Int).Mul(sharesBeforeSlash, slashDiff.WadSlashed), big.NewInt(-1e18))
					records = append(records, &StakerShareDeltas{
						Staker:          stakerShare.Staker,
						Strategy:        slashDiff.Strategy,
						Shares:          sharesSlashed.String(),
						StrategyIndex:   0,
						TransactionHash: slashDiff.TransactionHash,
						LogIndex:        slashDiff.LogIndex,
						BlockNumber:     slashDiff.BlockNumber,
					})

					// subtract (add the negative) the slashed shares from the net delta
					netDeltas[key] = netDeltas[key].Add(netDeltas[key], sharesSlashed)
				}
			}

			slashingIndex++
		}
	}
	if len(slashes) > 0 {
		ss.logger.Sugar().Debugw("Slashes found, printing records...")
		for _, r := range records {
			ss.logger.Sugar().Debugw("Staker shares",
				zap.String("staker", r.Staker),
				zap.String("strategy", r.Strategy),
				zap.String("shares", r.Shares),
				zap.Int("strategyIndex", int(r.StrategyIndex)),
				zap.String("transactionHash", r.TransactionHash),
				zap.Uint64("logIndex", r.LogIndex),
				zap.Uint64("blockNumber", r.BlockNumber),
			)
		}
	}
	return records, nil
}

func (ss *StakerSharesModel) writeDeltaRecords(blockNumber uint64) error {
	records, err := ss.prepareState(blockNumber)
	if err != nil {
		return err
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
		ss.logger.Sugar().Errorw("Failed to insert delta records",
			zap.String("blockNumber", fmt.Sprint(blockNumber)),
			zap.Error(res.Error),
		)

		var pgError *pgconn.PgError
		if errors.As(res.Error, &pgError) {
			ss.logger.Sugar().Errorw("Postgres error",
				zap.String("code", pgError.Code),
				zap.String("detail", pgError.Detail),
				zap.String("hint", pgError.Hint),
				zap.String("message", pgError.Message),
				zap.String("where", pgError.Where),
			)
		}
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

func (ss *StakerSharesModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := ss.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		ss.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
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

func (ss *StakerSharesModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var deltas []*StakerShareDeltas
	res := ss.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&deltas)
	if res.Error != nil {
		ss.logger.Sugar().Errorw("Failed to fetch staker share deltas", zap.Error(res.Error))
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(deltas), nil
}

func (ss *StakerSharesModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
