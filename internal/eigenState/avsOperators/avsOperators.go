package avsOperators

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/eigenStateModel"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/types"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/utils"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// Schema for registered_avs_operators block state table.
type RegisteredAvsOperators struct {
	Operator    string
	Avs         string
	BlockNumber uint64
	CreatedAt   time.Time
}

// AccumulatedStateChange represents the accumulated state change for a given block.
type AccumulatedStateChange struct {
	Avs         string
	Operator    string
	Registered  bool
	BlockNumber uint64
}

// RegisteredAvsOperatorDiff represents the diff between the registered_avs_operators table and the accumulated state.
type RegisteredAvsOperatorDiff struct {
	Avs         string
	Operator    string
	BlockNumber uint64
	Registered  bool
}

type AvsOperatorStateChange struct {
	Avs         string
	Operator    string
	Registered  bool
	LogIndex    uint64
	BlockNumber uint64
}

func NewSlotID(avs string, operator string) types.SlotID {
	return types.SlotID(fmt.Sprintf("%s_%s", avs, operator))
}

// EigenState model for AVS operators that implements IEigenStateModel.
type AvsOperatorsBaseModel struct {
	StateTransitions types.StateTransitions[AccumulatedStateChange]
	db               *gorm.DB
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*AccumulatedStateChange

	// Keep track of each distinct change, rather than accumulated change, to add to the delta table
	deltaAccumulator map[uint64][]*AvsOperatorStateChange
}

// NewAvsOperators creates a new AvsOperatorsBaseModel.
func NewAvsOperatorsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*eigenStateModel.EigenStateModel, error) {
	base := &AvsOperatorsBaseModel{
		db:           grm,
		logger:       logger,
		globalConfig: globalConfig,

		stateAccumulator: make(map[uint64]map[types.SlotID]*AccumulatedStateChange),

		deltaAccumulator: make(map[uint64][]*AvsOperatorStateChange),
	}
	m := eigenStateModel.NewEigenStateModel(base)

	esm.RegisterState(m, 0)
	return m, nil
}

func (a *AvsOperatorsBaseModel) Logger() *zap.Logger {
	return a.logger
}

func (a *AvsOperatorsBaseModel) ModelName() string {
	return "AvsOperatorsBaseModel"
}

func (a *AvsOperatorsBaseModel) TableName() string {
	return "registered_avs_operators"
}

func (a *AvsOperatorsBaseModel) DB() *gorm.DB {
	return a.db
}

func (a *AvsOperatorsBaseModel) Base() interface{} {
	return a
}

// Get the state transitions for the AvsOperatorsBaseModel state model
//
// Each state transition is function indexed by a block number.
// BlockNumber 0 is the catchall state
//
// Returns the map and a reverse sorted list of block numbers that can be traversed when
// processing a log to determine which state change to apply.
func (a *AvsOperatorsBaseModel) GetStateTransitions() (types.StateTransitions[AccumulatedStateChange], []uint64) {
	stateChanges := make(types.StateTransitions[AccumulatedStateChange])

	// TODO(seanmcgary): make this not a closure so this function doesnt get big an messy...
	stateChanges[0] = func(log *storage.TransactionLog) (*AccumulatedStateChange, error) {
		arguments, err := utils.ParseLogArguments(a.logger, log)
		if err != nil {
			return nil, err
		}

		outputData, err := utils.ParseLogOutput(a.logger, log)
		if err != nil {
			return nil, err
		}

		// Sanity check to make sure we've got an initialized accumulator map for the block
		if _, ok := a.stateAccumulator[log.BlockNumber]; !ok {
			return nil, xerrors.Errorf("No state accumulator found for block %d", log.BlockNumber)
		}

		avs := strings.ToLower(arguments[0].Value.(string))
		operator := strings.ToLower(arguments[1].Value.(string))

		registered := false
		if val, ok := outputData["status"]; ok {
			registered = uint64(val.(float64)) == 1
		}

		// Store the change in the delta accumulator
		a.deltaAccumulator[log.BlockNumber] = append(a.deltaAccumulator[log.BlockNumber], &AvsOperatorStateChange{
			Avs:         avs,
			Operator:    operator,
			Registered:  registered,
			LogIndex:    log.LogIndex,
			BlockNumber: log.BlockNumber,
		})

		slotID := NewSlotID(avs, operator)
		record, ok := a.stateAccumulator[log.BlockNumber][slotID]
		if !ok {
			record = &AccumulatedStateChange{
				Avs:         avs,
				Operator:    operator,
				BlockNumber: log.BlockNumber,
			}
			a.stateAccumulator[log.BlockNumber][slotID] = record
		}
		if !registered && ok {
			// In this situation, we've encountered a register and unregister in the same block
			// which functionally results in no state change at all so we want to remove the record
			// from the accumulated state.
			delete(a.stateAccumulator[log.BlockNumber], slotID)
			return nil, nil
		}
		record.Registered = registered

		return record, nil
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

// Returns a map of contract addresses to event names that are interesting to the state model.
func (a *AvsOperatorsBaseModel) GetInterestingLogMap() map[string][]string {
	contracts := a.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.AvsDirectory: {
			"OperatorAVSRegistrationStatusUpdated",
		},
	}
}

func (a *AvsOperatorsBaseModel) InitBlockProcessing(blockNumber uint64) error {
	a.stateAccumulator[blockNumber] = make(map[types.SlotID]*AccumulatedStateChange)
	a.deltaAccumulator[blockNumber] = make([]*AvsOperatorStateChange, 0)
	return nil
}

// Handle the state change for the given log
//
// Takes a log and iterates over the state transitions to determine which state change to apply based on block number.
func (a *AvsOperatorsBaseModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := a.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			a.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", blockNumber))

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

func (a *AvsOperatorsBaseModel) clonePreviousBlocksToNewBlock(blockNumber uint64) error {
	query := `
		insert into registered_avs_operators (avs, operator, block_number)
			select
				avs,
				operator,
				@currentBlock as block_number
			from registered_avs_operators
			where block_number = @previousBlock
	`
	res := a.db.Exec(query,
		sql.Named("currentBlock", blockNumber),
		sql.Named("previousBlock", blockNumber-1),
	)

	if res.Error != nil {
		a.logger.Sugar().Errorw("Failed to clone previous block state to new block", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

// prepareState prepares the state for the current block by comparing the accumulated state changes.
// It separates out the changes into inserts and deletes.
func (a *AvsOperatorsBaseModel) prepareState(blockNumber uint64) ([]RegisteredAvsOperators, []RegisteredAvsOperators, error) {
	accumulatedState, ok := a.stateAccumulator[blockNumber]
	if !ok {
		err := xerrors.Errorf("No accumulated state found for block %d", blockNumber)
		a.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, nil, err
	}

	inserts := make([]RegisteredAvsOperators, 0)
	deletes := make([]RegisteredAvsOperators, 0)
	for _, stateChange := range accumulatedState {
		record := RegisteredAvsOperators{
			Avs:         stateChange.Avs,
			Operator:    stateChange.Operator,
			BlockNumber: blockNumber,
		}
		if stateChange.Registered {
			inserts = append(inserts, record)
		} else {
			deletes = append(deletes, record)
		}
	}
	return inserts, deletes, nil
}

func (a *AvsOperatorsBaseModel) writeDeltaRecordsToDeltaTable(blockNumber uint64) error {
	records, ok := a.deltaAccumulator[blockNumber]
	if !ok {
		msg := "Delta accumulator was not initialized"
		a.logger.Sugar().Errorw(msg, zap.Uint64("blockNumber", blockNumber))
		return errors.New(msg)
	}

	if len(records) > 0 {
		res := a.db.Model(&AvsOperatorStateChange{}).Clauses(clause.Returning{}).Create(&records)
		if res.Error != nil {
			a.logger.Sugar().Errorw("Failed to insert delta records", zap.Error(res.Error))
			return res.Error
		}
	}
	return nil
}

// CommitFinalState commits the final state for the given block number.
func (a *AvsOperatorsBaseModel) CommitFinalState(blockNumber uint64) error {
	err := a.clonePreviousBlocksToNewBlock(blockNumber)
	if err != nil {
		return err
	}

	recordsToInsert, recordsToDelete, err := a.prepareState(blockNumber)
	if err != nil {
		return err
	}

	for _, record := range recordsToDelete {
		res := a.db.Delete(&RegisteredAvsOperators{}, "avs = ? and operator = ? and block_number = ?", record.Avs, record.Operator, record.BlockNumber)
		if res.Error != nil {
			a.logger.Sugar().Errorw("Failed to delete record",
				zap.Error(res.Error),
				zap.String("avs", record.Avs),
				zap.String("operator", record.Operator),
				zap.Uint64("blockNumber", blockNumber),
			)
			return res.Error
		}
	}
	if len(recordsToInsert) > 0 {
		res := a.db.Model(&RegisteredAvsOperators{}).Clauses(clause.Returning{}).Create(&recordsToInsert)
		if res.Error != nil {
			a.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
			return res.Error
		}
	}

	if err = a.writeDeltaRecordsToDeltaTable(blockNumber); err != nil {
		return err
	}

	return nil
}

func (a *AvsOperatorsBaseModel) ClearAccumulatedState(blockNumber uint64) error {
	delete(a.stateAccumulator, blockNumber)
	delete(a.deltaAccumulator, blockNumber)
	return nil
}

func (a *AvsOperatorsBaseModel) GetStateDiffs(blockNumber uint64) ([]types.StateDiff, error) {
	inserts, deletes, err := a.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	diffs := make([]*RegisteredAvsOperatorDiff, 0)
	for _, record := range inserts {
		diffs = append(diffs, &RegisteredAvsOperatorDiff{
			Avs:         record.Avs,
			Operator:    record.Operator,
			BlockNumber: record.BlockNumber,
			Registered:  true,
		})
	}
	for _, record := range deletes {
		diffs = append(diffs, &RegisteredAvsOperatorDiff{
			Avs:         record.Avs,
			Operator:    record.Operator,
			BlockNumber: record.BlockNumber,
			Registered:  false,
		})
	}

	stateDiffs := make([]types.StateDiff, 0)
	for _, diff := range diffs {
		stateDiffs = append(stateDiffs, types.StateDiff{
			SlotID: NewSlotID(diff.Avs, diff.Operator),
			Value:  []byte(fmt.Sprintf("%t", diff.Registered)),
		})
	}

	return stateDiffs, nil
}
