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
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/base"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/types"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"github.com/Layr-Labs/go-sidecar/internal/utils"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DelegatedStakers State model for staker delegations at block_number.
type DelegatedStakers struct {
	Staker      string
	Operator    string
	BlockNumber uint64
	CreatedAt   time.Time
}

// AccumulatedStateChange represents the accumulated state change for a staker/operator pair.
type AccumulatedStateChange struct {
	Staker      string
	Operator    string
	BlockNumber uint64
	Delegated   bool
}

type StakerDelegationChange struct {
	Staker      string
	Operator    string
	BlockNumber uint64
	Delegated   bool
	LogIndex    uint64
}

func NewSlotID(staker string, operator string) types.SlotID {
	return types.SlotID(fmt.Sprintf("%s_%s", staker, operator))
}

type StakerDelegationsModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[AccumulatedStateChange]
	DB               *gorm.DB
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*AccumulatedStateChange

	deltaAccumulator map[uint64][]*StakerDelegationChange
}

type DelegatedStakersDiff struct {
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
) (*StakerDelegationsModel, error) {
	model := &StakerDelegationsModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*AccumulatedStateChange),

		deltaAccumulator: make(map[uint64][]*StakerDelegationChange),
	}

	esm.RegisterState(model, 2)
	return model, nil
}

func (s *StakerDelegationsModel) GetModelName() string {
	return "StakerDelegationsModel"
}

func (s *StakerDelegationsModel) GetStateTransitions() (types.StateTransitions[AccumulatedStateChange], []uint64) {
	stateChanges := make(types.StateTransitions[AccumulatedStateChange])

	stateChanges[0] = func(log *storage.TransactionLog) (*AccumulatedStateChange, error) {
		arguments, err := s.ParseLogArguments(log)
		if err != nil {
			return nil, err
		}

		// Sanity check to make sure we've got an initialized accumulator map for the block
		if _, ok := s.stateAccumulator[log.BlockNumber]; !ok {
			return nil, xerrors.Errorf("No state accumulator found for block %d", log.BlockNumber)
		}

		staker := strings.ToLower(arguments[0].Value.(string))
		operator := strings.ToLower(arguments[1].Value.(string))

		slotID := NewSlotID(staker, operator)
		record, ok := s.stateAccumulator[log.BlockNumber][slotID]
		if !ok {
			// if the record doesn't exist, create a new one
			record = &AccumulatedStateChange{
				Staker:      staker,
				Operator:    operator,
				BlockNumber: log.BlockNumber,
			}
			s.stateAccumulator[log.BlockNumber][slotID] = record
		}
		if log.EventName == "StakerUndelegated" {
			if ok {
				// In this situation, we've encountered a delegate and undelegate in the same block
				// which functionally results in no state change at all so we want to remove the record
				// from the accumulated state.
				delete(s.stateAccumulator[log.BlockNumber], slotID)
				return nil, nil //nolint:nilnil
			}
			record.Delegated = false
		} else if log.EventName == "StakerDelegated" {
			record.Delegated = true
		}

		// Store the change in the delta accumulator
		s.deltaAccumulator[log.BlockNumber] = append(s.deltaAccumulator[log.BlockNumber], &StakerDelegationChange{
			Staker:      staker,
			Operator:    operator,
			BlockNumber: log.BlockNumber,
			Delegated:   record.Delegated,
			LogIndex:    log.LogIndex,
		})

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

func (s *StakerDelegationsModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := s.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.DelegationManager: {
			"StakerUndelegated",
			"StakerDelegated",
		},
	}
}

func (s *StakerDelegationsModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := s.getContractAddressesForEnvironment()
	return s.BaseEigenState.IsInterestingLog(addresses, log)
}

// InitBlock initialize state accumulator for the block.
func (s *StakerDelegationsModel) InitBlock(blockNumber uint64) error {
	s.stateAccumulator[blockNumber] = make(map[types.SlotID]*AccumulatedStateChange)
	s.deltaAccumulator[blockNumber] = make([]*StakerDelegationChange, 0)
	return nil
}

// CleanupBlock clears the accumulated state for the given block number to free up memory.
func (s *StakerDelegationsModel) CleanupBlock(blockNumber uint64) error {
	delete(s.stateAccumulator, blockNumber)
	delete(s.deltaAccumulator, blockNumber)
	return nil
}

func (s *StakerDelegationsModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := s.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			s.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", blockNumber))

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
	return nil, nil //nolint:nilnil
}

func (s *StakerDelegationsModel) clonePreviousBlocksToNewBlock(blockNumber uint64) error {
	query := `
		insert into delegated_stakers (staker, operator, block_number)
			select
				staker,
				operator,
				@currentBlock as block_number
			from delegated_stakers
			where block_number = @previousBlock
	`
	res := s.DB.Exec(query,
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
func (s *StakerDelegationsModel) prepareState(blockNumber uint64) ([]DelegatedStakers, []DelegatedStakers, error) {
	accumulatedState, ok := s.stateAccumulator[blockNumber]
	if !ok {
		err := xerrors.Errorf("No accumulated state found for block %d", blockNumber)
		s.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, nil, err
	}

	inserts := make([]DelegatedStakers, 0)
	deletes := make([]DelegatedStakers, 0)
	for _, stateChange := range accumulatedState {
		record := DelegatedStakers{
			Staker:      stateChange.Staker,
			Operator:    stateChange.Operator,
			BlockNumber: blockNumber,
		}
		if stateChange.Delegated {
			inserts = append(inserts, record)
		} else {
			deletes = append(deletes, record)
		}
	}
	return inserts, deletes, nil
}

func (s *StakerDelegationsModel) writeDeltaRecordsToDeltaTable(blockNumber uint64) error {
	records, ok := s.deltaAccumulator[blockNumber]
	if !ok {
		msg := "delta accumulator was not initialized"
		s.logger.Sugar().Errorw(msg, zap.Uint64("blockNumber", blockNumber))
		return errors.New(msg)
	}

	if len(records) > 0 {
		res := s.DB.Model(&StakerDelegationChange{}).Clauses(clause.Returning{}).Create(&records)
		if res.Error != nil {
			s.logger.Sugar().Errorw("Failed to insert delta records", zap.Error(res.Error))
			return res.Error
		}
	}
	return nil
}

func (s *StakerDelegationsModel) CommitFinalState(blockNumber uint64) error {
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
		res := s.DB.Delete(&DelegatedStakers{}, "staker = ? and operator = ? and block_number = ?", record.Staker, record.Operator, blockNumber)
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
		res := s.DB.Model(&DelegatedStakers{}).Clauses(clause.Returning{}).Create(&recordsToInsert)
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

// GenerateStateRoot generates the state root for the given block number by storing
// the state changes in a merkle tree.
func (s *StakerDelegationsModel) GenerateStateRoot(blockNumber uint64) (types.StateRoot, error) {
	inserts, deletes, err := s.prepareState(blockNumber)
	if err != nil {
		return "", err
	}

	// Take all of the inserts and deletes and combine them into a single list
	combinedResults := make([]DelegatedStakersDiff, 0)
	for _, record := range inserts {
		combinedResults = append(combinedResults, DelegatedStakersDiff{
			Staker:      record.Staker,
			Operator:    record.Operator,
			Delegated:   true,
			BlockNumber: blockNumber,
		})
	}
	for _, record := range deletes {
		combinedResults = append(combinedResults, DelegatedStakersDiff{
			Staker:      record.Staker,
			Operator:    record.Operator,
			Delegated:   false,
			BlockNumber: blockNumber,
		})
	}

	inputs := s.sortValuesForMerkleTree(combinedResults)

	fullTree, err := s.MerkleizeState(blockNumber, inputs)
	if err != nil {
		return "", err
	}
	return types.StateRoot(utils.ConvertBytesToString(fullTree.Root())), nil
}

func (s *StakerDelegationsModel) sortValuesForMerkleTree(diffs []DelegatedStakersDiff) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, diff := range diffs {
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: NewSlotID(diff.Staker, diff.Operator),
			Value:  []byte(fmt.Sprintf("%t", diff.Delegated)),
		})
	}
	slices.SortFunc(inputs, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})
	return inputs
}

func (s *StakerDelegationsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return s.BaseEigenState.DeleteState("delegated_stakers", startBlockNumber, endBlockNumber, s.DB)
}
