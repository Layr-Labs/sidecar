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

type SlashDiff struct {
	Operator        string
	Strategy        string
	WadsSlashed     *big.Int
	TransactionHash string
	LogIndex        uint64
	BlockTime       time.Time
	BlockDate       string
	BlockNumber     uint64
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

func NewShareDeltaSlotID(staker, strategy string) types.SlotID {
	return types.SlotID(fmt.Sprintf("%s_%s", staker, strategy))
}

type StakerShares struct {
	Staker      string
	Strategy    string
	Shares      string
	BlockNumber uint64
	CreatedAt   time.Time
}

type StakerSharesDiff struct {
	Staker      string
	Strategy    string
	Shares      *big.Int
	BlockNumber uint64
}

func StakerSharesToStakerSharesDiff(ss *StakerShares) (*StakerSharesDiff, error) {
	shares, success := numbers.NewBig257().SetString(ss.Shares, 10)
	if !success {
		return nil, xerrors.Errorf("Failed to convert shares to big.Int: %s", ss.Shares)
	}
	return &StakerSharesDiff{
		Staker:      ss.Staker,
		Strategy:    ss.Strategy,
		Shares:      shares,
		BlockNumber: ss.BlockNumber,
	}, nil
}

type StakerSharesModel struct {
	base.BaseEigenState
	DB           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	eventDeltaAccumulator map[uint64][]*StakerShareDeltas
	slashingAccumulator   map[uint64][]*SlashDiff

	deltaAccumulator map[uint64][]*StakerShareDeltas
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
		eventDeltaAccumulator: make(map[uint64][]*StakerShareDeltas),
		slashingAccumulator:   make(map[uint64][]*SlashDiff),
		deltaAccumulator:      make(map[uint64][]*StakerShareDeltas),
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
func (ss *StakerSharesModel) handleSlashingQueuedWithdrawal(log *storage.TransactionLog) ([]*StakerShareDeltas, error) {
	outputData, err := parseLogOutputForSlashingWithdrawalQueuedEvent(log.OutputData)
	if err != nil {
		return nil, err
	}

	records := make([]*StakerShareDeltas, 0)

	for i, strategy := range outputData.Withdrawal.Strategies {
		shares, success := numbers.NewBig257().SetString(outputData.SharesToWithdraw[i].String(), 10)
		if !success {
			return nil, xerrors.Errorf("Failed to convert shares to big.Int: %s", outputData.SharesToWithdraw[i])
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

type operatorSlashedOutputData struct {
	Operator    string        `json:"operator"`
	Strategies  []string      `json:"strategies"`
	WadsSlashed []json.Number `json:"wadsSlashed"`
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
		wadsSlashed, success := numbers.NewBig257().SetString(outputData.WadsSlashed[i].String(), 10)
		if !success {
			return nil, xerrors.Errorf("Failed to convert wadsSlashed to big.Int: %s", outputData.WadsSlashed[i])
		}
		stateDiffs = append(stateDiffs, &SlashDiff{
			Operator:        outputData.Operator,
			Strategy:        strategy,
			WadsSlashed:     wadsSlashed,
			TransactionHash: log.TransactionHash,
			LogIndex:        log.LogIndex,
			BlockNumber:     log.BlockNumber,
		})
	}

	return stateDiffs, nil
}

type AccumulatedStateDiffs struct {
	ShareDeltas []*StakerShareDeltas
	SlashDiffs  []*SlashDiff
}

func (ss *StakerSharesModel) GetStateTransitions() (types.StateTransitions[AccumulatedStateDiffs], []uint64) {
	stateChanges := make(types.StateTransitions[AccumulatedStateDiffs])

	stateChanges[0] = func(log *storage.TransactionLog) (*AccumulatedStateDiffs, error) {
		var shareDelta *StakerShareDeltas
		slashDiffs := make([]*SlashDiff, 0)
		shareDeltas := make([]*StakerShareDeltas, 0)
		var err error

		contractAddresses := ss.globalConfig.GetContractsMapForChain()

		// Staker shares is a bit more complex and has 4 possible contract/event combinations
		// that we need to handle
		if log.Address == contractAddresses.StrategyManager && log.EventName == "Deposit" {
			shareDelta, err = ss.handleStakerDepositEvent(log)
			shareDeltas = []*StakerShareDeltas{shareDelta}
		} else if log.Address == contractAddresses.EigenpodManager && log.EventName == "PodSharesUpdated" {
			shareDelta, err = ss.handlePodSharesUpdatedEvent(log)
			shareDeltas = []*StakerShareDeltas{shareDelta}
		} else if log.Address == contractAddresses.StrategyManager && log.EventName == "ShareWithdrawalQueued" && log.TransactionHash != "0x62eb0d0865b2636c74ed146e2d161e39e42b09bac7f86b8905fc7a830935dc1e" {
			shareDelta, err = ss.handleM1StakerWithdrawals(log)
			shareDeltas = []*StakerShareDeltas{shareDelta}
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "WithdrawalQueued" {
			shareDeltas, err = ss.handleM2QueuedWithdrawal(log)
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "WithdrawalMigrated" {
			shareDeltas, err = ss.handleMigratedM2StakerWithdrawals(log)
		} else if log.Address == contractAddresses.DelegationManager && log.EventName == "SlashingWithdrawalQueued" {
			shareDeltas, err = ss.handleSlashingQueuedWithdrawal(log)
		} else if log.Address == contractAddresses.AllocationManager && log.EventName == "OperatorSlashed" {
			slashDiffs, err = ss.handleOperatorSlashedEvent(log)
		} else {
			ss.logger.Sugar().Debugw("Got stakerShares event that we don't handle",
				zap.String("eventName", log.EventName),
				zap.String("address", log.Address),
			)
		}
		if err != nil {
			return nil, err
		}

		// Sanity check to make sure we've got an initialized accumulator maps for the block
		if _, ok := ss.eventDeltaAccumulator[log.BlockNumber]; !ok {
			return nil, xerrors.Errorf("no event delta accumulator found for block %d", log.BlockNumber)
		}
		if _, ok := ss.slashingAccumulator[log.BlockNumber]; !ok {
			return nil, xerrors.Errorf("no slashing accumulator found for block %d", log.BlockNumber)
		}

		// Add the share deltas to the accumulator
		ss.eventDeltaAccumulator[log.BlockNumber] = append(ss.eventDeltaAccumulator[log.BlockNumber], shareDeltas...)

		// Add the slashing diffs to the accumulator
		ss.slashingAccumulator[log.BlockNumber] = append(ss.slashingAccumulator[log.BlockNumber], slashDiffs...)

		return &AccumulatedStateDiffs{ShareDeltas: shareDeltas, SlashDiffs: slashDiffs}, nil
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
	ss.eventDeltaAccumulator[blockNumber] = make([]*StakerShareDeltas, 0)
	ss.slashingAccumulator[blockNumber] = make([]*SlashDiff, 0)
	ss.deltaAccumulator[blockNumber] = make([]*StakerShareDeltas, 0)
	return nil
}

func (ss *StakerSharesModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(ss.eventDeltaAccumulator, blockNumber)
	delete(ss.slashingAccumulator, blockNumber)
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

// TODO: this is not very performant in cases of large slashings and is called twice (once in CommitFinalState and once in GenerateStateRoot)
// but prob ok?
// prepareState prepares the state for commit by adding the new state to the existing state.
func (ss *StakerSharesModel) prepareState(blockNumber uint64) ([]*StakerSharesDiff, error) {
	// reset the delta accumulator
	ss.deltaAccumulator[blockNumber] = make([]*StakerShareDeltas, 0)
	// keep track of the updated staker shares
	updatedStakerShares := make(map[types.SlotID]*StakerSharesDiff)

	eventDeltaIndex := 0
	slashingIndex := 0
	for eventDeltaIndex < len(ss.eventDeltaAccumulator[blockNumber]) || slashingIndex < len(ss.slashingAccumulator[blockNumber]) {
		// initialize to max logIndex so we can compare
		eventShareDelta := &StakerShareDeltas{LogIndex: math.MaxUint64}
		slashDiff := &SlashDiff{LogIndex: math.MaxUint64}

		// load the accumulators if index exists
		if eventDeltaIndex < len(ss.eventDeltaAccumulator[blockNumber]) {
			eventShareDelta = ss.eventDeltaAccumulator[blockNumber][eventDeltaIndex]
		}

		if slashingIndex < len(ss.slashingAccumulator[blockNumber]) {
			slashDiff = ss.slashingAccumulator[blockNumber][slashingIndex]
		}

		if eventShareDelta.LogIndex < slashDiff.LogIndex {
			ss.logger.Sugar().Debugw(
				"Processing share delta",
				zap.String("staker", eventShareDelta.Staker),
				zap.String("strategy", eventShareDelta.Strategy),
				zap.String("shares", eventShareDelta.Shares))

			// if the shareDelta has a lower logIndex, process it
			slotID := NewShareDeltaSlotID(eventShareDelta.Staker, eventShareDelta.Strategy)

			// append the delta to the delta accumulator
			ss.deltaAccumulator[blockNumber] = append(ss.deltaAccumulator[blockNumber], eventShareDelta)

			// attempt to load from cache or load from db otherwise
			currStakerShares, ok := updatedStakerShares[slotID]
			if !ok {
				var err error
				currStakerShares, err = ss.GetStakerSharesForStakerAndStrategy(eventShareDelta.Staker, eventShareDelta.Strategy, blockNumber)
				if err != nil {
					return nil, err
				}
			}

			// add the shares to the existing shares
			shareDeltaShares, success := new(big.Int).SetString(eventShareDelta.Shares, 10)
			if !success {
				return nil, xerrors.Errorf("Failed to convert shares to big.Int: %s", eventShareDelta.Shares)
			}
			currStakerShares.Shares = currStakerShares.Shares.Add(currStakerShares.Shares, shareDeltaShares)
			currStakerShares.BlockNumber = blockNumber

			// update the local cache
			updatedStakerShares[slotID] = currStakerShares

			// increment the index
			eventDeltaIndex++
		} else if eventShareDelta.LogIndex > slashDiff.LogIndex {
			// if the slashDiff has a lower logIndex, process it
			stakerShares, err := ss.GetDelegatedStakerSharesToSlash(slashDiff)
			if err != nil {
				return nil, err
			}

			for _, stakerSharesRecord := range *stakerShares {
				ss.logger.Sugar().Debugw(
					"Processing slashing",
					zap.String("staker", stakerSharesRecord.Staker),
					zap.String("strategy", stakerSharesRecord.Strategy),
					zap.String("shares", stakerSharesRecord.Shares))

				slotID := NewShareDeltaSlotID(stakerSharesRecord.Staker, slashDiff.Strategy)
				var err error
				currStakerShares, ok := updatedStakerShares[slotID]
				// if we have not cached but the staker exists in the database for the strategy
				if !ok && stakerSharesRecord.Strategy == slashDiff.Strategy {
					currStakerShares, err = StakerSharesToStakerSharesDiff(&stakerSharesRecord)
					if err != nil {
						return nil, err
					}
				} else if !ok {
					// otherwise the staker is for a different strategy, so we skip
					continue
				}

				// use WAD = 1e18
				diffShares := new(big.Int).Div(new(big.Int).Mul(currStakerShares.Shares, slashDiff.WadsSlashed), big.NewInt(1e18))
				diffShares = diffShares.Mul(diffShares, big.NewInt(-1))

				// append the delta to the delta accumulator
				ss.deltaAccumulator[blockNumber] = append(ss.deltaAccumulator[blockNumber], &StakerShareDeltas{
					Staker:          stakerSharesRecord.Staker,
					Strategy:        slashDiff.Strategy,
					Shares:          diffShares.String(),
					StrategyIndex:   0,
					TransactionHash: slashDiff.TransactionHash,
					LogIndex:        slashDiff.LogIndex,
					BlockNumber:     slashDiff.BlockNumber,
				})

				// update the shares
				currStakerShares.Shares = currStakerShares.Shares.Add(currStakerShares.Shares, diffShares)
				currStakerShares.BlockNumber = blockNumber

				// update the local cache
				updatedStakerShares[slotID] = currStakerShares
			}

			// increment the index
			slashingIndex++
		}
	}

	// return the values as a slice
	preparedState := make([]*StakerSharesDiff, 0, len(updatedStakerShares))
	for _, v := range updatedStakerShares {
		ss.logger.Sugar().Debugw(
			"Prepared state",
			zap.String("staker", v.Staker),
			zap.String("strategy", v.Strategy),
			zap.String("shares", v.Shares.String()))
		preparedState = append(preparedState, v)
	}

	return preparedState, nil
}

// GetStakerSharesForStakerAndStrategy returns the latest shares for the staker in the
func (ss *StakerSharesModel) GetStakerSharesForStakerAndStrategy(staker, strategy string, blockNumber uint64) (*StakerSharesDiff, error) {
	stakerShares := &StakerShares{}
	// get the record from the database with the given staker and strategy with the latest blockNumber
	res := ss.DB.Model(&StakerShares{}).Where("staker = ? AND strategy = ?", staker, strategy).Order("block_number desc").First(&stakerShares)
	if res.Error != nil && res.Error != gorm.ErrRecordNotFound {
		ss.logger.Sugar().Errorw("Failed to fetch staker_shares", zap.Error(res.Error))
		return nil, res.Error
	}

	// if the record is not found, create a new record with shares as 0
	if res.Error == gorm.ErrRecordNotFound {
		stakerShares = &StakerShares{
			Staker:      staker,
			Strategy:    strategy,
			Shares:      "0",
			BlockNumber: blockNumber,
		}
	}

	return StakerSharesToStakerSharesDiff(stakerShares)
}

// GetDelegatedStakerSharesToSlash returns the staker shares for the stakers who were delegated to the operator that must be slashed for the
// given slashDiff returns a pointer to slice of their records for performance reasons
func (ss *StakerSharesModel) GetDelegatedStakerSharesToSlash(slashDiff *SlashDiff) (*[]StakerShares, error) {
	query := `
		with ranked_staker_delegations as (
			select
				staker,
				operator,
				delegated,
				ROW_NUMBER() OVER (PARTITION BY staker ORDER BY block_number desc, log_index desc) as rn
			from staker_delegation_changes
			where
				block_number <= @blockNumber and
				log_index <= @logIndex
		),
		delegated_stakers as (
			select
				lsd.staker
			from ranked_staker_delegations as lsd
			where
				lsd.operator = @operator
				and lsd.rn = 1
		),
		ranked_staker_shares as (
			select
				ds.staker,
				ss.strategy,
				COALESCE(ss.shares, 0) as shares,
				COALESCE(ss.block_number, 0) as block_number, -- Assign a default block number if needed
				ROW_NUMBER() OVER (
					PARTITION BY ds.staker, ss.strategy 
					ORDER BY ss.block_number desc
				) AS rn
			from 
				delegated_stakers as ds
			left join 
				staker_shares as ss
				on ss.staker = ds.staker
		),
		latest_staker_shares as (
			select
				ls.staker,
				ls.strategy,
				ls.shares,
				ls.block_number
			from ranked_staker_shares as ls
			where
				ls.rn = 1
		)
		select * from latest_staker_shares
	`
	stakerShares := make([]StakerShares, 0)

	// get the staker shares for the stakers who were delegated to the operator
	// and update the shares with the new max magnitude
	res := ss.DB.Raw(query,
		sql.Named("blockNumber", slashDiff.BlockNumber),
		sql.Named("logIndex", slashDiff.LogIndex),
		sql.Named("operator", slashDiff.Operator),
	).Scan(&stakerShares)
	if res.Error != nil {
		ss.logger.Sugar().Errorw("Failed to fetch staker_shares", zap.Error(res.Error))
		return nil, res.Error
	}

	return &stakerShares, nil
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
			SlotID: NewShareDeltaSlotID(diff.Staker, diff.Strategy),
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
