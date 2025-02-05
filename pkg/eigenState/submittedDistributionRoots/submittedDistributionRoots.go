package submittedDistributionRoots

import (
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SubmittedDistributionRootsModel struct {
	base.BaseEigenState
	DB           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*types.SubmittedDistributionRoot
	committedState   map[uint64][]*types.SubmittedDistributionRoot
}

func NewSubmittedDistributionRootsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*SubmittedDistributionRootsModel, error) {
	model := &SubmittedDistributionRootsModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*types.SubmittedDistributionRoot),
		committedState:   make(map[uint64][]*types.SubmittedDistributionRoot),
	}

	esm.RegisterState(model, 4)
	return model, nil
}

const SubmittedDistributionRootsModelName = "SubmittedDistributionRootsModel"

func (sdr *SubmittedDistributionRootsModel) GetModelName() string {
	return SubmittedDistributionRootsModelName
}

type distributionRootSubmittedOutput struct {
	ActivatedAt uint64 `json:"activatedAt"`
}

func parseLogOutputForDistributionRootSubmitted(outputDataStr string) (*distributionRootSubmittedOutput, error) {
	outputData := &distributionRootSubmittedOutput{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}
	return outputData, err
}

func (sdr *SubmittedDistributionRootsModel) GetStateTransitions() (types.StateTransitions[*types.SubmittedDistributionRoot], []uint64) {
	stateChanges := make(types.StateTransitions[*types.SubmittedDistributionRoot])

	stateChanges[0] = func(log *storage.TransactionLog) (*types.SubmittedDistributionRoot, error) {
		arguments, err := sdr.ParseLogArguments(log)
		if err != nil {
			return nil, err
		}
		outputData, err := parseLogOutputForDistributionRootSubmitted(log.OutputData)
		if err != nil {
			return nil, err
		}

		// Sanity check to make sure we've got an initialized accumulator map for the block
		if _, ok := sdr.stateAccumulator[log.BlockNumber]; !ok {
			return nil, fmt.Errorf("No state accumulator found for block %d", log.BlockNumber)
		}

		var rootIndex uint64

		t := reflect.TypeOf(arguments[0].Value)
		switch t.Kind() {
		case reflect.String:
			if arguments[0].Value.(string) == "0x0000000000000000000000000000000000000000" {
				rootIndex = 0
				break
			}
			withoutPrefix := strings.TrimPrefix(arguments[0].Value.(string), "0x")
			rootIndex, err = strconv.ParseUint(withoutPrefix, 16, 32)
			if err != nil {
				return nil, fmt.Errorf("Failed to decode rootIndex: %v", err)
			}
		case reflect.Float64:
			rootIndex = uint64(arguments[0].Value.(float64))
		default:
			return nil, fmt.Errorf("Invalid type for rootIndex: %s", t.Kind())
		}

		root := arguments[1].Value.(string)

		var rewardsCalculationEnd int64
		calcualtionEndType := reflect.TypeOf(arguments[2].Value)
		switch calcualtionEndType.Kind() {
		case reflect.String:
			withoutPrefix := strings.TrimPrefix(arguments[2].Value.(string), "0x")
			decoded, err := strconv.ParseUint(withoutPrefix, 16, 32)
			if err != nil {
				return nil, fmt.Errorf("Failed to decode rewardsCalculationEnd: %v", err)
			}
			rewardsCalculationEnd = int64(decoded)
		case reflect.Float64:
			rewardsCalculationEnd = int64(arguments[2].Value.(float64))
		default:
			return nil, fmt.Errorf("Invalid type for rewardsCalculationEnd: %s", calcualtionEndType.Kind())
		}

		activatedAt := outputData.ActivatedAt

		slotId := base.NewSlotID(log.TransactionHash, log.LogIndex)
		_, ok := sdr.stateAccumulator[log.BlockNumber][slotId]
		if ok {
			err := fmt.Errorf("Duplicate distribution root submitted for slot %s at block %d", slotId, log.BlockNumber)
			sdr.logger.Sugar().Errorw("Duplicate distribution root submitted", zap.Error(err))
			return nil, err
		}

		record := &types.SubmittedDistributionRoot{
			Root:                      root,
			BlockNumber:               log.BlockNumber,
			RootIndex:                 rootIndex,
			RewardsCalculationEnd:     time.Unix(rewardsCalculationEnd, 0),
			RewardsCalculationEndUnit: "snapshot",
			ActivatedAt:               time.Unix(int64(activatedAt), 0),
			ActivatedAtUnit:           "timestamp",
			CreatedAtBlockNumber:      log.BlockNumber,
			TransactionHash:           log.TransactionHash,
			LogIndex:                  log.LogIndex,
		}
		sdr.stateAccumulator[log.BlockNumber][slotId] = record

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

func (sdr *SubmittedDistributionRootsModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := sdr.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.RewardsCoordinator: {
			"DistributionRootSubmitted",
		},
	}
}

func (sdr *SubmittedDistributionRootsModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := sdr.getContractAddressesForEnvironment()
	return sdr.BaseEigenState.IsInterestingLog(addresses, log)
}

func (sdr *SubmittedDistributionRootsModel) SetupStateForBlock(blockNumber uint64) error {
	sdr.stateAccumulator[blockNumber] = make(map[types.SlotID]*types.SubmittedDistributionRoot)
	sdr.committedState = make(map[uint64][]*types.SubmittedDistributionRoot)
	return nil
}

func (sdr *SubmittedDistributionRootsModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(sdr.stateAccumulator, blockNumber)
	delete(sdr.committedState, blockNumber)
	return nil
}

func (sdr *SubmittedDistributionRootsModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := sdr.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			sdr.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

			change, err := stateChanges[blockNumber](log)
			if err != nil {
				return nil, err
			}
			if change == nil {
				sdr.logger.Sugar().Debugw("No state change found", zap.Uint64("blockNumber", blockNumber))
				return nil, nil
			}
			return change, nil
		}
	}
	return nil, nil
}

// prepareState prepares the state for commit by adding the new state to the existing state.
func (sdr *SubmittedDistributionRootsModel) prepareState(blockNumber uint64) ([]*types.SubmittedDistributionRoot, error) {
	preparedState := make([]*types.SubmittedDistributionRoot, 0)

	accumulatedState, ok := sdr.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		sdr.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	for _, newState := range accumulatedState {
		preparedState = append(preparedState, newState)
	}
	return preparedState, nil
}

func (sdr *SubmittedDistributionRootsModel) CommitFinalState(blockNumber uint64) error {
	records, err := sdr.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(records) > 0 {
		res := sdr.DB.Model(&types.SubmittedDistributionRoot{}).Clauses(clause.Returning{}).Create(&records)
		if res.Error != nil {
			sdr.logger.Sugar().Errorw("Failed to create new submitted_distribution_roots records", zap.Error(res.Error))
			return res.Error
		}
	}
	sdr.committedState[blockNumber] = records

	return nil
}

func (sdr *SubmittedDistributionRootsModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := sdr.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		sdr.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (sdr *SubmittedDistributionRootsModel) sortValuesForMerkleTree(inputs []*types.SubmittedDistributionRoot) []*base.MerkleTreeInput {
	values := make([]*base.MerkleTreeInput, 0)
	for _, input := range inputs {
		values = append(values, &base.MerkleTreeInput{
			SlotID: base.NewSlotID(input.TransactionHash, input.LogIndex),
			Value:  []byte(input.Root),
		})
	}
	slices.SortFunc(values, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})
	return values
}

func (sdr *SubmittedDistributionRootsModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	diffs, err := sdr.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	sortedInputs := sdr.sortValuesForMerkleTree(diffs)

	if len(sortedInputs) == 0 {
		return nil, nil
	}

	fullTree, err := sdr.MerkleizeEigenState(blockNumber, sortedInputs)
	if err != nil {
		sdr.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", sortedInputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (sdr *SubmittedDistributionRootsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return sdr.BaseEigenState.DeleteState("submitted_distribution_roots", startBlockNumber, endBlockNumber, sdr.DB)
}

func (sdr *SubmittedDistributionRootsModel) GetAccumulatedState(blockNumber uint64) []*types.SubmittedDistributionRoot {
	s, ok := sdr.stateAccumulator[blockNumber]
	if !ok {
		return nil
	}
	states := make([]*types.SubmittedDistributionRoot, 0)
	for _, state := range s {
		states = append(states, state)
	}
	return states
}

func (sdr *SubmittedDistributionRootsModel) GetSubmittedRootsForBlock(blockNumber uint64) ([]*types.SubmittedDistributionRoot, error) {
	records := make([]*types.SubmittedDistributionRoot, 0)
	res := sdr.DB.Model(&types.SubmittedDistributionRoot{}).
		Where("block_number = ?", blockNumber).
		Find(&records)
	if res.Error != nil {
		return nil, res.Error
	}
	return records, nil
}

func (sdr *SubmittedDistributionRootsModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var deltas []*types.SubmittedDistributionRoot
	res := sdr.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&deltas)
	if res.Error != nil {
		sdr.logger.Sugar().Errorw("Failed to list deltas for block range",
			zap.Error(res.Error),
			zap.Uint64("startBlockNumber", startBlockNumber),
			zap.Uint64("endBlockNumber", endBlockNumber),
		)
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(deltas), nil
}

func (sdr *SubmittedDistributionRootsModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
