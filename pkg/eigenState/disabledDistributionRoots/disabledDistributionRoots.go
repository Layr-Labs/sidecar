package disabledDistributionRoots

import (
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/go-sidecar/pkg/storage"
	"github.com/Layr-Labs/go-sidecar/pkg/utils"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"slices"
	"sort"
)

func NewSlotID(rootIndex uint64) types.SlotID {
	return types.SlotID(fmt.Sprintf("%d", rootIndex))
}

type DisabledDistributionRootsModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[types.DisabledDistributionRoot]
	DB               *gorm.DB
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*types.DisabledDistributionRoot
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
	}

	esm.RegisterState(model, 6)
	return model, nil
}

const MODEL_NAME = "DisabledDistributionRootsModel"

func (ddr *DisabledDistributionRootsModel) GetModelName() string {
	return MODEL_NAME
}

func (ddr *DisabledDistributionRootsModel) GetStateTransitions() (types.StateTransitions[types.DisabledDistributionRoot], []uint64) {
	stateChanges := make(types.StateTransitions[types.DisabledDistributionRoot])

	stateChanges[0] = func(log *storage.TransactionLog) (*types.DisabledDistributionRoot, error) {
		arguments, err := ddr.ParseLogArguments(log)
		if err != nil {
			return nil, err
		}
		// Sanity check to make sure we've got an initialized accumulator map for the block
		if _, ok := ddr.stateAccumulator[log.BlockNumber]; !ok {
			return nil, xerrors.Errorf("No state accumulator found for block %d", log.BlockNumber)
		}

		// json numbers are float64s but we want a uint64
		rootIndex := uint64(arguments[0].Value.(float64))

		slotId := NewSlotID(rootIndex)
		_, ok := ddr.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := xerrors.Errorf("Duplicate disabledDistributionRoot for slot %s at block %d", slotId, log.BlockNumber)
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
	return nil
}

func (ddr *DisabledDistributionRootsModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(ddr.stateAccumulator, blockNumber)
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
		err := xerrors.Errorf("No accumulated state found for block %d", blockNumber)
		ddr.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	for _, state := range accumulatedState {
		preparedState = append(preparedState, state)
	}

	return preparedState, nil
}

func (ddr *DisabledDistributionRootsModel) CommitFinalState(blockNumber uint64) error {
	records, err := ddr.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(records) > 0 {
		res := ddr.DB.Model(&types.DisabledDistributionRoot{}).Clauses(clause.Returning{}).Create(&records)
		if res.Error != nil {
			ddr.logger.Sugar().Errorw("Failed to create new submitted_distribution_roots records", zap.Error(res.Error))
			return res.Error
		}
	}

	return nil
}

func (ddr *DisabledDistributionRootsModel) sortValuesForMerkleTree(inputs []*types.DisabledDistributionRoot) []*base.MerkleTreeInput {
	slices.SortFunc(inputs, func(i, j *types.DisabledDistributionRoot) int {
		return int(i.RootIndex - j.RootIndex)
	})

	values := make([]*base.MerkleTreeInput, 0)
	for _, input := range inputs {
		values = append(values, &base.MerkleTreeInput{
			SlotID: NewSlotID(input.RootIndex),
			Value:  []byte("disabled"),
		})
	}
	return values
}

func (ddr *DisabledDistributionRootsModel) GenerateStateRoot(blockNumber uint64) (types.StateRoot, error) {
	diffs, err := ddr.prepareState(blockNumber)
	if err != nil {
		return "", err
	}

	sortedInputs := ddr.sortValuesForMerkleTree(diffs)

	fullTree, err := ddr.MerkleizeState(blockNumber, sortedInputs)
	if err != nil {
		return "", err
	}
	return types.StateRoot(utils.ConvertBytesToString(fullTree.Root())), nil
}

func (ddr *DisabledDistributionRootsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return ddr.BaseEigenState.DeleteState("disabled_distribution_roots", startBlockNumber, endBlockNumber, ddr.DB)
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

// IncludeStateRootForBlock returns true if the state root should be included for the given block number.
func (ddr *DisabledDistributionRootsModel) IncludeStateRootForBlock(blockNumber uint64) bool {
	switch ddr.globalConfig.Chain {
	case config.Chain_Mainnet:
		return blockNumber >= 20872746 // block of the first root that we disabled
	}
	return true
}