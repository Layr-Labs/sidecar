package stakerDelegations

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

// StakerDelegationRecord record for staker delegations at block_number.
type StakerDelegationRecord struct {
	Staker      string
	Operator    string
	BlockNumber uint64
	CreatedAt   time.Time
}

type StakerDelegationDelta struct {
	Staker      string
	Operator    string
	BlockNumber uint64
	Delegated   bool
	LogIndex    uint64
}

func NewSlotID(staker string, operator string) types.SlotID {
	return types.SlotID(fmt.Sprintf("%s_%s", staker, operator))
}

type StakerDelegationsBaseModel struct {
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	deltaAccumulator map[uint64][]*StakerDelegationDelta
}

type StakerDelegationStateDiff struct {
	Staker      string
	Operator    string
	Delegated   bool
	BlockNumber uint64
}

func NewStakerDelegationsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*eigenStateModel.EigenStateModel, error) {
	base := &StakerDelegationsBaseModel{
		db:           grm,
		logger:       logger,
		globalConfig: globalConfig,

		deltaAccumulator: make(map[uint64][]*StakerDelegationDelta),
	}
	m := eigenStateModel.NewEigenStateModel(base)

	esm.RegisterState(m, 2)
	return m, nil
}

func (s *StakerDelegationsBaseModel) Logger() *zap.Logger {
	return s.logger
}

func (s *StakerDelegationsBaseModel) ModelName() string {
	return "StakerDelegationsBaseModel"
}

func (s *StakerDelegationsBaseModel) TableName() string {
	return "delegated_stakers"
}

func (s *StakerDelegationsBaseModel) DB() *gorm.DB {
	return s.db
}

func (s *StakerDelegationsBaseModel) Base() interface{} {
	return s
}

func (s *StakerDelegationsBaseModel) GetStateTransitions() (types.StateTransitions, []uint64) {
	stateChanges := make(types.StateTransitions)

	stateChanges[0] = func(log *storage.TransactionLog) (interface{}, error) {
		arguments, err := utils.ParseLogArguments(s.logger, log)
		if err != nil {
			return nil, err
		}

		staker := strings.ToLower(arguments[0].Value.(string))
		operator := strings.ToLower(arguments[1].Value.(string))

		delta := &StakerDelegationDelta{
			Staker:      staker,
			Operator:    operator,
			BlockNumber: log.BlockNumber,
			Delegated:   log.EventName == "StakerDelegated", // the event name determines if the staker was delegated or undelegated
			LogIndex:    log.LogIndex,
		}
		// Store the change in the delta accumulator
		s.deltaAccumulator[log.BlockNumber] = append(s.deltaAccumulator[log.BlockNumber], delta)
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

func (s *StakerDelegationsBaseModel) GetInterestingLogMap() map[string][]string {
	contracts := s.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.DelegationManager: {
			"StakerUndelegated",
			"StakerDelegated",
		},
	}
}

// InitBlock initialize state accumulator for the block.
func (s *StakerDelegationsBaseModel) InitBlock(blockNumber uint64) error {
	s.deltaAccumulator[blockNumber] = make([]*StakerDelegationDelta, 0)
	return nil
}

// CleanupBlock clears the accumulated state for the given block number to free up memory.
func (s *StakerDelegationsBaseModel) CleanupBlock(blockNumber uint64) error {
	delete(s.deltaAccumulator, blockNumber)
	return nil
}

func (s *StakerDelegationsBaseModel) clonePreviousBlocksToNewBlock(blockNumber uint64) error {
	query := `
		insert into delegated_stakers (staker, operator, block_number)
			select
				staker,
				operator,
				@currentBlock as block_number
			from delegated_stakers
			where block_number = @previousBlock
	`
	res := s.db.Exec(query,
		sql.Named("currentBlock", blockNumber),
		sql.Named("previousBlock", blockNumber-1),
	)

	if res.Error != nil {
		s.logger.Sugar().Errorw("Failed to clone previous block state to new block", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

// prepareState prepares the state for the current block by comparing the accumulated state changes.
// It separates out the changes into inserts and deletes.
func (s *StakerDelegationsBaseModel) prepareState(blockNumber uint64) ([]StakerDelegationDelta, []StakerDelegationDelta, error) {
	accumulatedDeltas, ok := s.deltaAccumulator[blockNumber]
	if !ok {
		err := xerrors.Errorf("No accumulated deltas found for block %d", blockNumber)
		s.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, nil, err
	}

	deltaMap := make(map[types.SlotID]StakerDelegationDelta)
	for _, delta := range accumulatedDeltas {
		slotID := NewSlotID(delta.Staker, delta.Operator)
		prevDelta, ok := deltaMap[slotID]
		// if we have a previous delta for this slot, and it was delegated, and the current delta is not delegated
		// then we can remove the slot from the map
		if ok && prevDelta.Delegated && !delta.Delegated {
			delete(deltaMap, slotID)
		} else {
			deltaMap[slotID] = *delta
		}
	}

	inserts := make([]StakerDelegationDelta, 0)
	deletes := make([]StakerDelegationDelta, 0)
	for _, delta := range deltaMap {
		if delta.Delegated {
			inserts = append(inserts, delta)
		} else {
			deletes = append(deletes, delta)
		}
	}

	return inserts, deletes, nil
}

func (s *StakerDelegationsBaseModel) writeDeltaRecordsToDeltaTable(blockNumber uint64) error {
	records, ok := s.deltaAccumulator[blockNumber]
	if !ok {
		msg := "delta accumulator was not initialized"
		s.logger.Sugar().Errorw(msg, zap.Uint64("blockNumber", blockNumber))
		return errors.New(msg)
	}

	if len(records) > 0 {
		res := s.db.Model(&StakerDelegationDelta{}).Clauses(clause.Returning{}).Create(&records)
		if res.Error != nil {
			s.logger.Sugar().Errorw("Failed to insert delta records", zap.Error(res.Error))
			return res.Error
		}
	}
	return nil
}

func (s *StakerDelegationsBaseModel) CommitFinalState(blockNumber uint64) error {
	// Clone the previous block state to give us a reference point.
	//
	// By doing this, existing staker delegations will be carried over to the new block.
	// We'll then remove any stakers that were undelegated and add any new stakers that were delegated.
	err := s.clonePreviousBlocksToNewBlock(blockNumber)
	if err != nil {
		return err
	}

	recordsToInsert, recordsToDelete, err := s.prepareState(blockNumber)
	if err != nil {
		return err
	}

	// TODO(seanmcgary): should probably wrap the operations of this function in a db transaction
	for _, record := range recordsToDelete {
		res := s.db.Delete(&StakerDelegationRecord{}, "staker = ? and operator = ? and block_number = ?", record.Staker, record.Operator, blockNumber)
		if res.Error != nil {
			s.logger.Sugar().Errorw("Failed to delete staker delegation",
				zap.Error(res.Error),
				zap.String("staker", record.Staker),
				zap.String("operator", record.Operator),
				zap.Uint64("blockNumber", blockNumber),
			)
			return res.Error
		}
	}
	if len(recordsToInsert) > 0 {
		res := s.db.Model(&StakerDelegationRecord{}).Clauses(clause.Returning{}).Create(&recordsToInsert)
		if res.Error != nil {
			s.logger.Sugar().Errorw("Failed to insert staker delegations", zap.Error(res.Error))
			return res.Error
		}
	}

	if err = s.writeDeltaRecordsToDeltaTable(blockNumber); err != nil {
		return err
	}
	return nil
}

func (s *StakerDelegationsBaseModel) GetStateDiffs(blockNumber uint64) ([]types.StateDiff, error) {
	inserts, deletes, err := s.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	// Take all of the inserts and deletes and combine them into a single list
	diffs := make([]StakerDelegationStateDiff, 0)
	for _, record := range inserts {
		diffs = append(diffs, StakerDelegationStateDiff{
			Staker:      record.Staker,
			Operator:    record.Operator,
			Delegated:   true,
			BlockNumber: blockNumber,
		})
	}
	for _, record := range deletes {
		diffs = append(diffs, StakerDelegationStateDiff{
			Staker:      record.Staker,
			Operator:    record.Operator,
			Delegated:   false,
			BlockNumber: blockNumber,
		})
	}

	stateDiffs := make([]types.StateDiff, 0)
	for _, diff := range diffs {
		stateDiffs = append(stateDiffs, types.StateDiff{
			SlotID: NewSlotID(diff.Staker, diff.Operator),
			Value:  []byte(fmt.Sprintf("%t", diff.Delegated)),
		})
	}

	return stateDiffs, nil
}
