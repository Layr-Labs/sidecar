package avsOperators

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	pkgUtils "github.com/Layr-Labs/go-sidecar/pkg/utils"

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
type AvsOperatorRegistrationRecord struct {
	Operator    string
	Avs         string
	BlockNumber uint64
	CreatedAt   time.Time
}

type AvsOperatorRegistrationDelta struct {
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
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	// Keep track of each distinct change, rather than accumulated change, to add to the delta table
	deltaAccumulator map[uint64][]*AvsOperatorRegistrationDelta
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

		deltaAccumulator: make(map[uint64][]*AvsOperatorRegistrationDelta),
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
func (a *AvsOperatorsBaseModel) GetStateTransitions() (types.StateTransitions, []uint64) {
	stateChanges := make(types.StateTransitions)

	// TODO(seanmcgary): make this not a closure so this function doesnt get big an messy...
	stateChanges[0] = func(log *storage.TransactionLog) (interface{}, error) {
		arguments, err := utils.ParseLogArguments(a.logger, log)
		if err != nil {
			return nil, err
		}

		outputData, err := utils.ParseLogOutput(a.logger, log)
		if err != nil {
			return nil, err
		}

		avs := strings.ToLower(arguments[0].Value.(string))
		operator := strings.ToLower(arguments[1].Value.(string))

		registered := false
		if val, ok := outputData["status"]; ok {
			registered = uint64(val.(float64)) == 1
		}

		// Store the change in the delta accumulator
		delta := &AvsOperatorRegistrationDelta{
			Avs:         avs,
			Operator:    operator,
			Registered:  registered,
			LogIndex:    log.LogIndex,
			BlockNumber: log.BlockNumber,
		}
		a.deltaAccumulator[log.BlockNumber] = append(a.deltaAccumulator[log.BlockNumber], delta)

		return delta, nil
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

func (a *AvsOperatorsBaseModel) InitBlock(blockNumber uint64) error {
	a.deltaAccumulator[blockNumber] = make([]*AvsOperatorRegistrationDelta, 0)
	return nil
}

func (a *AvsOperatorsBaseModel) CleanupBlock(blockNumber uint64) error {
	delete(a.deltaAccumulator, blockNumber)
	return nil
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
func (a *AvsOperatorsBaseModel) prepareState(blockNumber uint64) ([]AvsOperatorRegistrationDelta, []AvsOperatorRegistrationDelta, error) {
	accumulatedDeltas, ok := a.deltaAccumulator[blockNumber]
	if !ok {
		msg := "delta accumulator was not initialized"
		a.logger.Sugar().Errorw(msg, zap.Uint64("blockNumber", blockNumber))
		return nil, nil, errors.New(msg)
	}

	deltaMap := make(map[types.SlotID]AvsOperatorRegistrationDelta)
	for _, delta := range accumulatedDeltas {
		slotID := NewSlotID(delta.Avs, delta.Operator)
		prevDelta, ok := deltaMap[slotID]
		// if we have a deregistration followed from a previous registration for the same avs/operator pair, we can ignore both
		if ok && prevDelta.Registered && !delta.Registered {
			delete(deltaMap, slotID)
		} else {
			deltaMap[slotID] = *delta
		}
	}

	inserts := make([]AvsOperatorRegistrationDelta, 0)
	deletes := make([]AvsOperatorRegistrationDelta, 0)

	for _, delta := range deltaMap {
		if delta.Registered {
			inserts = append(inserts, delta)
		} else {
			deletes = append(deletes, delta)
		}
	}

	return inserts, deletes, nil
}

func (a *AvsOperatorsBaseModel) writeDeltaRecordsToDeltaTable(blockNumber uint64) error {
	records, ok := a.deltaAccumulator[blockNumber]
	if !ok {
		msg := "delta accumulator was not initialized"
		a.logger.Sugar().Errorw(msg, zap.Uint64("blockNumber", blockNumber))
		return errors.New(msg)
	}

	if len(records) > 0 {
		res := a.db.Model(&AvsOperatorRegistrationDelta{}).Clauses(clause.Returning{}).Create(&records)
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

	inserts, deletes, err := a.prepareState(blockNumber)
	if err != nil {
		return err
	}

	for _, record := range deletes {
		res := a.db.Delete(&AvsOperatorRegistrationRecord{}, "avs = ? and operator = ? and block_number = ?", record.Avs, record.Operator, record.BlockNumber)
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

	recordsToInsert := pkgUtils.Map(inserts, func(delta AvsOperatorRegistrationDelta, i uint64) AvsOperatorRegistrationRecord {
		return AvsOperatorRegistrationRecord{
			Avs:         delta.Avs,
			Operator:    delta.Operator,
			BlockNumber: blockNumber,
		}
	})

	if len(inserts) > 0 {
		res := a.db.Model(&AvsOperatorRegistrationRecord{}).Clauses(clause.Returning{}).Create(&recordsToInsert)
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

func (a *AvsOperatorsBaseModel) GetStateDiffs(blockNumber uint64) ([]types.StateDiff, error) {
	inserts, deletes, err := a.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	stateDiffs := make([]types.StateDiff, 0)
	for _, record := range inserts {
		stateDiffs = append(stateDiffs, types.StateDiff{
			SlotID: NewSlotID(record.Avs, record.Operator),
			Value:  []byte(fmt.Sprintf("%t", true)),
		})
	}
	for _, record := range deletes {
		stateDiffs = append(stateDiffs, types.StateDiff{
			SlotID: NewSlotID(record.Avs, record.Operator),
			Value:  []byte(fmt.Sprintf("%t", false)),
		})
	}

	return stateDiffs, nil
}
