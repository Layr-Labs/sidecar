package disabledDistributionRoots

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"slices"
	"sort"
	"strings"
)

type DisabledDistributionRootsModel struct {
	base.BaseEigenState
	DB           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*types.DisabledDistributionRoot
	committedState   map[uint64][]*types.DisabledDistributionRoot
}

func NewDisabledDistributionRootsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*DisabledDistributionRootsModel, error) {
	model := &DisabledDistributionRootsModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*types.DisabledDistributionRoot),
		committedState:   make(map[uint64][]*types.DisabledDistributionRoot),
	}

	esm.RegisterState(model, 6)
	return model, nil
}

const DisabledDistributionRootsModelName = "DisabledDistributionRootsModel"

func (ddr *DisabledDistributionRootsModel) GetModelName() string {
	return DisabledDistributionRootsModelName
}

func (ddr *DisabledDistributionRootsModel) GetStateTransitions() (types.StateTransitions[*types.DisabledDistributionRoot], []uint64) {
	stateChanges := make(types.StateTransitions[*types.DisabledDistributionRoot])

	stateChanges[0] = func(log *storage.TransactionLog) (*types.DisabledDistributionRoot, error) {
		arguments, err := ddr.ParseLogArguments(log)
		if err != nil {
			return nil, err
		}
		// Sanity check to make sure we've got an initialized accumulator map for the block
		if _, ok := ddr.stateAccumulator[log.BlockNumber]; !ok {
			return nil, fmt.Errorf("No state accumulator found for block %d", log.BlockNumber)
		}

		// json numbers are float64s but we want a uint64
		rootIndex := uint64(arguments[0].Value.(float64))

		slotId := base.NewSlotID(log.TransactionHash, log.LogIndex)
		_, ok := ddr.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate disabledDistributionRoot for slot %s at block %d", slotId, log.BlockNumber)
			ddr.logger.Sugar().Errorw("Duplicate disabledDistributionRoot", zap.Error(err))
			return nil, err
		}

		record := &types.DisabledDistributionRoot{
			BlockNumber:     log.BlockNumber,
			RootIndex:       rootIndex,
			LogIndex:        log.LogIndex,
			TransactionHash: log.TransactionHash,
		}
		ddr.stateAccumulator[log.BlockNumber][slotId] = record

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

func (ddr *DisabledDistributionRootsModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := ddr.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.RewardsCoordinator: {
			"DistributionRootDisabled",
		},
	}
}

func (ddr *DisabledDistributionRootsModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := ddr.getContractAddressesForEnvironment()
	return ddr.BaseEigenState.IsInterestingLog(addresses, log)
}

func (ddr *DisabledDistributionRootsModel) SetupStateForBlock(blockNumber uint64) error {
	ddr.stateAccumulator[blockNumber] = make(map[types.SlotID]*types.DisabledDistributionRoot)
	ddr.committedState[blockNumber] = make([]*types.DisabledDistributionRoot, 0)
	return nil
}

func (ddr *DisabledDistributionRootsModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(ddr.stateAccumulator, blockNumber)
	delete(ddr.committedState, blockNumber)
	return nil
}

func (ddr *DisabledDistributionRootsModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := ddr.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			ddr.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

			change, err := stateChanges[blockNumber](log)
			if err != nil {
				return nil, err
			}
			if change == nil {
				ddr.logger.Sugar().Debugw("No state change found", zap.Uint64("blockNumber", blockNumber))
				return nil, nil
			}
			return change, nil
		}
	}
	return nil, nil
}

// prepareState prepares the state for commit by adding the new state to the existing state.
func (ddr *DisabledDistributionRootsModel) prepareState(blockNumber uint64) ([]*types.DisabledDistributionRoot, error) {
	preparedState := make([]*types.DisabledDistributionRoot, 0)

	accumulatedState, ok := ddr.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		ddr.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	for _, state := range accumulatedState {
		preparedState = append(preparedState, state)
	}

	return preparedState, nil
}

func (ddr *DisabledDistributionRootsModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	records, err := ddr.prepareState(blockNumber)
	if err != nil {
		return err
	}

	insertedRecords, err := base.CommitFinalState(records, ignoreInsertConflicts, ddr.GetTableName(), ddr.DB)
	if err != nil {
		ddr.logger.Sugar().Errorw("Failed to create new submitted_distribution_roots records", zap.Error(err))
		return err
	}
	ddr.committedState[blockNumber] = insertedRecords

	return nil
}

func (ddr *DisabledDistributionRootsModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := ddr.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		ddr.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (ddr *DisabledDistributionRootsModel) sortValuesForMerkleTree(inputs []*types.DisabledDistributionRoot) []*base.MerkleTreeInput {
	values := make([]*base.MerkleTreeInput, 0)
	for _, input := range inputs {
		values = append(values, &base.MerkleTreeInput{
			SlotID: base.NewSlotID(input.TransactionHash, input.LogIndex),
			Value:  []byte(fmt.Sprintf("%d", input.RootIndex)),
		})
	}
	slices.SortFunc(values, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})
	return values
}

func (ddr *DisabledDistributionRootsModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	diffs, err := ddr.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	sortedInputs := ddr.sortValuesForMerkleTree(diffs)

	if len(sortedInputs) == 0 {
		return nil, nil
	}

	fullTree, err := ddr.MerkleizeEigenState(blockNumber, sortedInputs)
	if err != nil {
		ddr.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", sortedInputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (ddr *DisabledDistributionRootsModel) GetTableName() string {
	return "disabled_distribution_roots"
}

func (ddr *DisabledDistributionRootsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return ddr.BaseEigenState.DeleteState(ddr.GetTableName(), startBlockNumber, endBlockNumber, ddr.DB)
}

func (ddr *DisabledDistributionRootsModel) GetAccumulatedState(blockNumber uint64) []*types.DisabledDistributionRoot {
	s, ok := ddr.stateAccumulator[blockNumber]
	if !ok {
		return nil
	}
	states := make([]*types.DisabledDistributionRoot, 0)
	for _, state := range s {
		states = append(states, state)
	}
	return states
}

func (ddr *DisabledDistributionRootsModel) GetSubmittedRootsForBlock(blockNumber uint64) ([]*types.DisabledDistributionRoot, error) {
	records := make([]*types.DisabledDistributionRoot, 0)
	res := ddr.DB.Model(&types.DisabledDistributionRoot{}).
		Where("block_number = ?", blockNumber).
		Find(&records)
	if res.Error != nil {
		return nil, res.Error
	}
	return records, nil
}

func (ddr *DisabledDistributionRootsModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	records := make([]*types.DisabledDistributionRoot, 0)
	res := ddr.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&records)
	if res.Error != nil {
		ddr.logger.Sugar().Errorw("Failed to list records for block range",
			zap.Error(res.Error),
			zap.Uint64("startBlockNumber", startBlockNumber),
			zap.Uint64("endBlockNumber", endBlockNumber),
		)
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (ddr *DisabledDistributionRootsModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
