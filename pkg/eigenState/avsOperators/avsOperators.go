package avsOperators

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

type AvsOperatorStateChange struct {
	Avs             string
	Operator        string
	Registered      bool
	LogIndex        uint64
	TransactionHash string
	BlockNumber     uint64
}

// EigenState model for AVS operators that implements IEigenStateModel.
type AvsOperatorsModel struct {
	base.BaseEigenState
	DB           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	// Keep track of each distinct change, rather than accumulated change, to add to the delta table
	stateAccumulator map[uint64][]*AvsOperatorStateChange
	committedState   map[uint64][]*AvsOperatorStateChange
}

// NewAvsOperatorsModel creates a new AvsOperatorsModel.
func NewAvsOperatorsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*AvsOperatorsModel, error) {
	s := &AvsOperatorsModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:           grm,
		logger:       logger,
		globalConfig: globalConfig,

		stateAccumulator: make(map[uint64][]*AvsOperatorStateChange),
		committedState:   make(map[uint64][]*AvsOperatorStateChange),
	}
	esm.RegisterState(s, 0)
	return s, nil
}

const AvsOperatorsModelName = "AvsOperatorsModel"

func (a *AvsOperatorsModel) GetModelName() string {
	return AvsOperatorsModelName
}

// Get the state transitions for the AvsOperatorsModel state model
//
// Each state transition is function indexed by a block number.
// BlockNumber 0 is the catchall state
//
// Returns the map and a reverse sorted list of block numbers that can be traversed when
// processing a log to determine which state change to apply.
func (a *AvsOperatorsModel) GetStateTransitions() (types.StateTransitions[*AvsOperatorStateChange], []uint64) {
	stateChanges := make(types.StateTransitions[*AvsOperatorStateChange])

	stateChanges[0] = func(log *storage.TransactionLog) (*AvsOperatorStateChange, error) {
		arguments, err := a.ParseLogArguments(log)
		if err != nil {
			return nil, err
		}

		outputData, err := a.ParseLogOutput(log)
		if err != nil {
			return nil, err
		}

		// Sanity check to make sure we've got an initialized accumulator map for the block
		if _, ok := a.stateAccumulator[log.BlockNumber]; !ok {
			return nil, fmt.Errorf("No state accumulator found for block %d", log.BlockNumber)
		}

		operator := strings.ToLower(arguments[0].Value.(string))
		avs := strings.ToLower(arguments[1].Value.(string))

		registered := false
		if val, ok := outputData["status"]; ok {
			registered = uint64(val.(float64)) == 1
		}

		// Store the change in the delta accumulator
		delta := &AvsOperatorStateChange{
			Avs:             avs,
			Operator:        operator,
			Registered:      registered,
			LogIndex:        log.LogIndex,
			BlockNumber:     log.BlockNumber,
			TransactionHash: log.TransactionHash,
		}
		a.stateAccumulator[log.BlockNumber] = append(a.stateAccumulator[log.BlockNumber], delta)

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
func (a *AvsOperatorsModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := a.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.AvsDirectory: {
			"OperatorAVSRegistrationStatusUpdated",
		},
	}
}

// Given a log, determine if it is interesting to the state model.
func (a *AvsOperatorsModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := a.getContractAddressesForEnvironment()
	return a.BaseEigenState.IsInterestingLog(addresses, log)
}

func (a *AvsOperatorsModel) SetupStateForBlock(blockNumber uint64) error {
	a.stateAccumulator[blockNumber] = make([]*AvsOperatorStateChange, 0)
	a.committedState[blockNumber] = make([]*AvsOperatorStateChange, 0)
	return nil
}

func (a *AvsOperatorsModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(a.stateAccumulator, blockNumber)
	delete(a.committedState, blockNumber)
	return nil
}

// Handle the state change for the given log
//
// Takes a log and iterates over the state transitions to determine which state change to apply based on block number.
func (a *AvsOperatorsModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := a.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			a.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

			change, err := stateChanges[blockNumber](log)
			if err != nil {
				return nil, err
			}

			if change == nil {
				a.logger.Sugar().Debugw("No state change found", zap.Uint64("blockNumber", blockNumber))
				return nil, nil
			}
			return change, nil
		}
	}
	return nil, nil
}

// prepareState prepares the state for the current block by comparing the accumulated state changes.
// It separates out the changes into inserts and deletes.
func (a *AvsOperatorsModel) prepareState(blockNumber uint64) ([]*AvsOperatorStateChange, error) {
	accumulatedState, ok := a.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		a.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	return accumulatedState, nil
}

func (a *AvsOperatorsModel) writeDeltaRecords(blockNumber uint64) error {
	records, ok := a.stateAccumulator[blockNumber]
	if !ok {
		msg := "delta accumulator was not initialized"
		a.logger.Sugar().Errorw(msg, zap.Uint64("blockNumber", blockNumber))
		return errors.New(msg)
	}

	if len(records) > 0 {
		res := a.DB.Model(&AvsOperatorStateChange{}).Clauses(clause.Returning{}).Create(&records)
		if res.Error != nil {
			a.logger.Sugar().Errorw("Failed to insert delta records", zap.Error(res.Error))
			return res.Error
		}
	}
	a.committedState[blockNumber] = records
	return nil
}

// CommitFinalState commits the final state for the given block number.
func (a *AvsOperatorsModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	if err := a.writeDeltaRecords(blockNumber); err != nil {
		return err
	}

	return nil
}

func (a *AvsOperatorsModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := a.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		a.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (a *AvsOperatorsModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	deltas, err := a.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs := a.sortValuesForMerkleTree(deltas)

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := a.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		a.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (a *AvsOperatorsModel) sortValuesForMerkleTree(deltas []*AvsOperatorStateChange) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, d := range deltas {
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: base.NewSlotID(d.TransactionHash, d.LogIndex),
			Value:  []byte(fmt.Sprintf("%t", d.Registered)),
		})
	}
	slices.SortFunc(inputs, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})
	return inputs
}

func (a *AvsOperatorsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return a.BaseEigenState.DeleteState("avs_operator_state_changes", startBlockNumber, endBlockNumber, a.DB)
}

func (a *AvsOperatorsModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	records := make([]*AvsOperatorStateChange, 0)
	res := a.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&records)
	if res.Error != nil {
		a.logger.Sugar().Errorw("Failed to list records for block range",
			zap.Error(res.Error),
			zap.Uint64("startBlockNumber", startBlockNumber),
			zap.Uint64("endBlockNumber", endBlockNumber),
		)
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (a *AvsOperatorsModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
