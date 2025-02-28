package stakerDelegations

import (
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"slices"
	"sort"
	"strings"
)

type StakerDelegationChange struct {
	Staker           string
	Operator         string
	BlockNumber      uint64
	Delegated        bool
	LogIndex         uint64
	TransactionHash  string
	TransactionIndex uint64 `gorm:"-"`
}

type StakerDelegationsModel struct {
	base.BaseEigenState
	DB           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	stateAccumulator map[uint64][]*StakerDelegationChange
	committedState   map[uint64][]*StakerDelegationChange
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
		DB:           grm,
		logger:       logger,
		globalConfig: globalConfig,

		stateAccumulator: make(map[uint64][]*StakerDelegationChange),
		committedState:   make(map[uint64][]*StakerDelegationChange),
	}

	esm.RegisterState(model, 2)
	return model, nil
}

const StakerDelegationsModelName = "StakerDelegationsModel"

func (s *StakerDelegationsModel) GetModelName() string {
	return StakerDelegationsModelName
}

func (s *StakerDelegationsModel) GetStateTransitions() (types.StateTransitions[*StakerDelegationChange], []uint64) {
	stateChanges := make(types.StateTransitions[*StakerDelegationChange])

	stateChanges[0] = func(log *storage.TransactionLog) (*StakerDelegationChange, error) {
		arguments, err := s.ParseLogArguments(log)
		if err != nil {
			return nil, err
		}

		// Sanity check to make sure we've got an initialized accumulator map for the block
		if _, ok := s.stateAccumulator[log.BlockNumber]; !ok {
			return nil, fmt.Errorf("No state accumulator found for block %d", log.BlockNumber)
		}

		staker := strings.ToLower(arguments[0].Value.(string))
		operator := strings.ToLower(arguments[1].Value.(string))

		delta := &StakerDelegationChange{
			Staker:           staker,
			Operator:         operator,
			BlockNumber:      log.BlockNumber,
			LogIndex:         log.LogIndex,
			TransactionHash:  log.TransactionHash,
			TransactionIndex: log.TransactionIndex,
		}
		if log.EventName == "StakerUndelegated" {
			delta.Delegated = false
		} else if log.EventName == "StakerDelegated" {
			delta.Delegated = true
		}

		// Store the change in the delta accumulator
		s.stateAccumulator[log.BlockNumber] = append(s.stateAccumulator[log.BlockNumber], delta)

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

// SetupStateForBlock initialize state accumulator for the block.
func (s *StakerDelegationsModel) SetupStateForBlock(blockNumber uint64) error {
	s.stateAccumulator[blockNumber] = make([]*StakerDelegationChange, 0)
	s.committedState[blockNumber] = make([]*StakerDelegationChange, 0)
	return nil
}

// CleanupProcessedStateForBlock clears the accumulated state for the given block number to free up memory.
func (s *StakerDelegationsModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(s.stateAccumulator, blockNumber)
	delete(s.committedState, blockNumber)
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
				s.logger.Sugar().Debugw("No state change found", zap.Uint64("blockNumber", blockNumber))
				return nil, nil
			}
			return change, nil
		}
	}
	return nil, nil //nolint:nilnil
}

func (s *StakerDelegationsModel) prepareState(blockNumber uint64) ([]*StakerDelegationChange, error) {
	deltas, ok := s.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		s.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	return deltas, nil
}

func (s *StakerDelegationsModel) writeDeltaRecords(blockNumber uint64) error {
	records, ok := s.stateAccumulator[blockNumber]
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
	if err := s.writeDeltaRecords(blockNumber); err != nil {
		return err
	}
	return nil
}

func (s *StakerDelegationsModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := s.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		s.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

// GenerateStateRoot generates the state root for the given block number by storing
// the state changes in a merkle tree.
func (s *StakerDelegationsModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	deltas, err := s.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs := s.sortValuesForMerkleTree(deltas)

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := s.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		s.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (s *StakerDelegationsModel) sortValuesForMerkleTree(deltas []*StakerDelegationChange) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, d := range deltas {
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: base.NewSlotID(d.TransactionHash, d.LogIndex),
			Value:  []byte(fmt.Sprintf("%t", d.Delegated)),
		})
	}
	slices.SortFunc(inputs, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})
	return inputs
}

func (s *StakerDelegationsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return s.BaseEigenState.DeleteState("staker_delegation_changes", startBlockNumber, endBlockNumber, s.DB)
}

func (s *StakerDelegationsModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var deltas []*StakerDelegationChange
	res := s.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&deltas)
	if res.Error != nil {
		s.logger.Sugar().Errorw("Failed to list deltas for block range",
			zap.Error(res.Error),
			zap.Uint64("startBlockNumber", startBlockNumber),
			zap.Uint64("endBlockNumber", endBlockNumber),
		)
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(deltas), nil
}

func (s *StakerDelegationsModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}

func (s *StakerDelegationsModel) GetAccumulatedState(blockNumber uint64) []*StakerDelegationChange {
	return s.stateAccumulator[blockNumber]
}
