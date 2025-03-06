package rewardSubmissions

import (
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/types/numbers"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type RewardSubmission struct {
	Avs             string
	RewardHash      string
	Token           string
	Amount          string
	Strategy        string
	StrategyIndex   uint64
	Multiplier      string
	StartTimestamp  *time.Time
	EndTimestamp    *time.Time
	Duration        uint64
	BlockNumber     uint64
	RewardType      string // avs, all_stakers, all_earners
	TransactionHash string
	LogIndex        uint64
}

func NewSlotID(transactionHash string, logIndex uint64, rewardHash string, strategyIndex uint64) types.SlotID {
	return base.NewSlotIDWithSuffix(transactionHash, logIndex, fmt.Sprintf("%s_%016x", rewardHash, strategyIndex))
}

type RewardSubmissionsModel struct {
	base.BaseEigenState
	DB           *gorm.DB
	Network      config.Network
	Environment  config.Environment
	logger       *zap.Logger
	globalConfig *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*RewardSubmission
	committedState   map[uint64][]*RewardSubmission
}

func NewRewardSubmissionsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*RewardSubmissionsModel, error) {
	model := &RewardSubmissionsModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*RewardSubmission),
		committedState:   make(map[uint64][]*RewardSubmission),
	}

	esm.RegisterState(model, 5)
	return model, nil
}

const RewardSubmissionsModelName = "RewardSubmissionsModel"

func (rs *RewardSubmissionsModel) GetModelName() string {
	return RewardSubmissionsModelName
}

type genericRewardPaymentData struct {
	Token                    string      `json:"token"`
	Amount                   json.Number `json:"amount"`
	StartTimestamp           uint64      `json:"startTimestamp"`
	Duration                 uint64      `json:"duration"`
	StrategiesAndMultipliers []struct {
		Strategy   string      `json:"strategy"`
		Multiplier json.Number `json:"multiplier"`
	} `json:"strategiesAndMultipliers"`
}

type rewardSubmissionOutputData struct {
	RewardsSubmission *genericRewardPaymentData `json:"rewardsSubmission"`
	RangePayment      *genericRewardPaymentData `json:"rangePayment"`
}

func parseRewardSubmissionOutputData(outputDataStr string) (*rewardSubmissionOutputData, error) {
	outputData := &rewardSubmissionOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (rs *RewardSubmissionsModel) handleRewardSubmissionCreatedEvent(log *storage.TransactionLog) ([]*RewardSubmission, error) {
	arguments, err := rs.ParseLogArguments(log)
	if err != nil {
		return nil, err
	}

	outputData, err := parseRewardSubmissionOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}

	var actualOuputData *genericRewardPaymentData
	if log.EventName == "RangePaymentCreated" || log.EventName == "RangePaymentForAllCreated" {
		actualOuputData = outputData.RangePayment
	} else {
		actualOuputData = outputData.RewardsSubmission
	}

	if actualOuputData.Duration == 0 {
		rs.logger.Sugar().Debugw("Skipping reward submission with zero duration",
			zap.Uint64("blockNumber", log.BlockNumber),
			zap.String("transactionHash", log.TransactionHash),
			zap.Uint64("logIndex", log.LogIndex),
		)
		return []*RewardSubmission{}, nil
	}

	rewardSubmissions := make([]*RewardSubmission, 0)

	for i, strategyAndMultiplier := range actualOuputData.StrategiesAndMultipliers {
		startTimestamp := time.Unix(int64(actualOuputData.StartTimestamp), 0)
		endTimestamp := startTimestamp.Add(time.Duration(actualOuputData.Duration) * time.Second)

		amountBig, success := numbers.NewBig257().SetString(actualOuputData.Amount.String(), 10)
		if !success {
			return nil, fmt.Errorf("Failed to parse amount to Big257: %s", actualOuputData.Amount.String())
		}

		multiplierBig, success := numbers.NewBig257().SetString(strategyAndMultiplier.Multiplier.String(), 10)
		if !success {
			return nil, fmt.Errorf("Failed to parse multiplier to Big257: %s", actualOuputData.Amount.String())
		}

		var rewardType string
		if log.EventName == "RewardsSubmissionForAllCreated" || log.EventName == "RangePaymentForAllCreated" {
			rewardType = "all_stakers"
		} else if log.EventName == "RangePaymentCreated" || log.EventName == "AVSRewardsSubmissionCreated" {
			rewardType = "avs"
		} else if log.EventName == "RewardsSubmissionForAllEarnersCreated" {
			rewardType = "all_earners"
		} else {
			return nil, fmt.Errorf("Unknown event name: %s", log.EventName)
		}

		rewardSubmission := &RewardSubmission{
			Avs:             strings.ToLower(arguments[0].Value.(string)),
			RewardHash:      strings.ToLower(arguments[2].Value.(string)),
			Token:           strings.ToLower(actualOuputData.Token),
			Amount:          amountBig.String(),
			Strategy:        strategyAndMultiplier.Strategy,
			Multiplier:      multiplierBig.String(),
			StartTimestamp:  &startTimestamp,
			EndTimestamp:    &endTimestamp,
			Duration:        actualOuputData.Duration,
			BlockNumber:     log.BlockNumber,
			RewardType:      rewardType,
			TransactionHash: log.TransactionHash,
			LogIndex:        log.LogIndex,
			StrategyIndex:   uint64(i),
		}
		rewardSubmissions = append(rewardSubmissions, rewardSubmission)
	}

	return rewardSubmissions, nil
}

func (rs *RewardSubmissionsModel) GetStateTransitions() (types.StateTransitions[[]*RewardSubmission], []uint64) {
	stateChanges := make(types.StateTransitions[[]*RewardSubmission])

	stateChanges[0] = func(log *storage.TransactionLog) ([]*RewardSubmission, error) {
		rewardSubmissions, err := rs.handleRewardSubmissionCreatedEvent(log)
		if err != nil {
			return nil, err
		}

		for _, rewardSubmission := range rewardSubmissions {
			slotId := NewSlotID(rewardSubmission.TransactionHash, rewardSubmission.LogIndex, rewardSubmission.RewardHash, rewardSubmission.StrategyIndex)

			_, ok := rs.stateAccumulator[log.BlockNumber][slotId]
			if ok {
				fmt.Printf("Submissions: %+v\n", rs.stateAccumulator[log.BlockNumber])
				err := fmt.Errorf("Duplicate distribution root submitted for slot %s at block %d", slotId, log.BlockNumber)
				rs.logger.Sugar().Errorw("Duplicate distribution root submitted", zap.Error(err))
				return nil, err
			}

			rs.stateAccumulator[log.BlockNumber][slotId] = rewardSubmission
		}

		return rewardSubmissions, nil
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

func (rs *RewardSubmissionsModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := rs.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.RewardsCoordinator: {
			"RangePaymentForAllCreated",
			"RewardsSubmissionForAllCreated",
			"RangePaymentCreated",
			"AVSRewardsSubmissionCreated",
			"RewardsSubmissionForAllEarnersCreated",
		},
	}
}

func (rs *RewardSubmissionsModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := rs.getContractAddressesForEnvironment()
	return rs.BaseEigenState.IsInterestingLog(addresses, log)
}

func (rs *RewardSubmissionsModel) SetupStateForBlock(blockNumber uint64) error {
	rs.stateAccumulator[blockNumber] = make(map[types.SlotID]*RewardSubmission)
	rs.committedState[blockNumber] = make([]*RewardSubmission, 0)
	return nil
}

func (rs *RewardSubmissionsModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(rs.stateAccumulator, blockNumber)
	delete(rs.committedState, blockNumber)
	return nil
}

func (rs *RewardSubmissionsModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := rs.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			rs.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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

// prepareState prepares the state for commit by adding the new state to the existing state.
func (rs *RewardSubmissionsModel) prepareState(blockNumber uint64) ([]*RewardSubmission, error) {
	accumulatedState, ok := rs.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		rs.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*RewardSubmission, 0)
	for _, submission := range accumulatedState {
		recordsToInsert = append(recordsToInsert, submission)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (rs *RewardSubmissionsModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	recordsToInsert, err := rs.prepareState(blockNumber)
	if err != nil {
		return err
	}

	insertedRecords, err := base.CommitFinalState(recordsToInsert, ignoreInsertConflicts, rs.GetTableName(), rs.DB)
	if err != nil {
		rs.logger.Sugar().Errorw("Failed to commit final state", zap.Error(err))
		return err
	}
	rs.committedState[blockNumber] = insertedRecords

	return nil
}

func (rs *RewardSubmissionsModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := rs.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		rs.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (rs *RewardSubmissionsModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := rs.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs := rs.sortValuesForMerkleTree(inserts)

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := rs.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		rs.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (rs *RewardSubmissionsModel) sortValuesForMerkleTree(submissions []*RewardSubmission) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, submission := range submissions {
		slotID := NewSlotID(submission.TransactionHash, submission.LogIndex, submission.RewardHash, submission.StrategyIndex)
		value := "added"
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: slotID,
			Value:  []byte(value),
		})
	}

	slices.SortFunc(inputs, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})

	return inputs
}

func (rs *RewardSubmissionsModel) GetTableName() string {
	return "reward_submissions"
}

func (rs *RewardSubmissionsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return rs.BaseEigenState.DeleteState(rs.GetTableName(), startBlockNumber, endBlockNumber, rs.DB)
}

func (rs *RewardSubmissionsModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	var submissions []*RewardSubmission
	res := rs.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&submissions)
	if res.Error != nil {
		rs.logger.Sugar().Errorw("Failed to list records", zap.Error(res.Error))
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(submissions), nil
}

func (rs *RewardSubmissionsModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
