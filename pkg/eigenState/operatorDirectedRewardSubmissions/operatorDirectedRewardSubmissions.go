package operatorDirectedRewardSubmissions

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Layr-Labs/go-sidecar/pkg/storage"
	"github.com/Layr-Labs/go-sidecar/pkg/types/numbers"
	"github.com/Layr-Labs/go-sidecar/pkg/utils"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/types"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type OperatorDirectedRewardSubmission struct {
	Avs            string
	RewardHash     string
	Token          string
	Operator       string
	OperatorIndex  uint64
	Amount         string
	Strategy       string
	StrategyIndex  uint64
	Multiplier     string
	StartTimestamp *time.Time
	EndTimestamp   *time.Time
	Duration       uint64
	BlockNumber    uint64
}

type RewardSubmissionDiff struct {
	OperatorDirectedRewardSubmission *OperatorDirectedRewardSubmission
	IsNew                            bool
	IsNoLongerActive                 bool
}

type OperatorDirectedRewardSubmissions struct {
	Submissions []*OperatorDirectedRewardSubmission
}

func NewSlotID(rewardHash string, strategy string, operator string) types.SlotID {
	return types.SlotID(fmt.Sprintf("%s_%s_%s", rewardHash, strategy, operator))
}

type OperatorDirectedRewardSubmissionsModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[OperatorDirectedRewardSubmission]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*OperatorDirectedRewardSubmission
}

func NewOperatorDirectedRewardSubmissionsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*OperatorDirectedRewardSubmissionsModel, error) {
	model := &OperatorDirectedRewardSubmissionsModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*OperatorDirectedRewardSubmission),
	}

	esm.RegisterState(model, 7)
	return model, nil
}

func (odrs *OperatorDirectedRewardSubmissionsModel) GetModelName() string {
	return "OperatorDirectedRewardSubmissionsModel"
}

type genericRewardPaymentData struct {
	Token                    string
	Amount                   json.Number
	StartTimestamp           uint64
	Duration                 uint64
	StrategiesAndMultipliers []struct {
		Strategy   string
		Multiplier json.Number
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

func (odrs *OperatorDirectedRewardSubmissionsModel) handleRewardSubmissionCreatedEvent(log *storage.TransactionLog) (*OperatorDirectedRewardSubmissions, error) {
	arguments, err := odrs.ParseLogArguments(log)
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

	rewardSubmissions := make([]*OperatorDirectedRewardSubmission, 0)

	for _, strategyAndMultiplier := range actualOuputData.StrategiesAndMultipliers {
		startTimestamp := time.Unix(int64(actualOuputData.StartTimestamp), 0)
		endTimestamp := startTimestamp.Add(time.Duration(actualOuputData.Duration) * time.Second)

		amountBig, success := numbers.NewBig257().SetString(actualOuputData.Amount.String(), 10)
		if !success {
			return nil, xerrors.Errorf("Failed to parse amount to Big257: %s", actualOuputData.Amount.String())
		}

		multiplierBig, success := numbers.NewBig257().SetString(strategyAndMultiplier.Multiplier.String(), 10)
		if !success {
			return nil, xerrors.Errorf("Failed to parse multiplier to Big257: %s", actualOuputData.Amount.String())
		}

		var rewardType string
		if log.EventName == "RewardsSubmissionForAllCreated" || log.EventName == "RangePaymentForAllCreated" {
			rewardType = "all_stakers"
		} else if log.EventName == "RangePaymentCreated" || log.EventName == "AVSRewardsSubmissionCreated" {
			rewardType = "avs"
		} else if log.EventName == "RewardsSubmissionForAllEarnersCreated" {
			rewardType = "all_earners"
		} else {
			return nil, xerrors.Errorf("Unknown event name: %s", log.EventName)
		}

		rewardSubmission := &OperatorDirectedRewardSubmission{
			Avs:            strings.ToLower(arguments[0].Value.(string)),
			RewardHash:     strings.ToLower(arguments[2].Value.(string)),
			Token:          strings.ToLower(actualOuputData.Token),
			Amount:         amountBig.String(),
			Strategy:       strategyAndMultiplier.Strategy,
			Multiplier:     multiplierBig.String(),
			StartTimestamp: &startTimestamp,
			EndTimestamp:   &endTimestamp,
			Duration:       actualOuputData.Duration,
			BlockNumber:    log.BlockNumber,
			RewardType:     rewardType,
		}
		rewardSubmissions = append(rewardSubmissions, rewardSubmission)
	}

	return &OperatorDirectedRewardSubmissions{Submissions: rewardSubmissions}, nil
}

func (odrs *OperatorDirectedRewardSubmissionsModel) GetStateTransitions() (types.StateTransitions[OperatorDirectedRewardSubmissions], []uint64) {
	stateChanges := make(types.StateTransitions[OperatorDirectedRewardSubmissions])

	stateChanges[0] = func(log *storage.TransactionLog) (*OperatorDirectedRewardSubmissions, error) {
		rewardSubmissions, err := odrs.handleRewardSubmissionCreatedEvent(log)
		if err != nil {
			return nil, err
		}

		for _, rewardSubmission := range rewardSubmissions.Submissions {
			slotId := NewSlotID(rewardSubmission.RewardHash, rewardSubmission.Strategy, rewardSubmission.Operator)

			_, ok := odrs.stateAccumulator[log.BlockNumber][slotId]
			if ok {
				err := xerrors.Errorf("Duplicate distribution root submitted for slot %s at block %d", slotId, log.BlockNumber)
				odrs.logger.Sugar().Errorw("Duplicate distribution root submitted", zap.Error(err))
				return nil, err
			}

			odrs.stateAccumulator[log.BlockNumber][slotId] = rewardSubmission
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

func (odrs *OperatorDirectedRewardSubmissionsModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := odrs.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.RewardsCoordinator: {
			"OperatorDirectedAVSRewardsSubmissionCreated",
		},
	}
}

func (odrs *OperatorDirectedRewardSubmissionsModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := odrs.getContractAddressesForEnvironment()
	return odrs.BaseEigenState.IsInterestingLog(addresses, log)
}

func (odrs *OperatorDirectedRewardSubmissionsModel) SetupStateForBlock(blockNumber uint64) error {
	odrs.stateAccumulator[blockNumber] = make(map[types.SlotID]*OperatorDirectedRewardSubmission)
	return nil
}

func (odrs *OperatorDirectedRewardSubmissionsModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(odrs.stateAccumulator, blockNumber)
	return nil
}

func (odrs *OperatorDirectedRewardSubmissionsModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := odrs.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			odrs.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (odrs *OperatorDirectedRewardSubmissionsModel) prepareState(blockNumber uint64) ([]*RewardSubmissionDiff, []*RewardSubmissionDiff, error) {
	accumulatedState, ok := odrs.stateAccumulator[blockNumber]
	if !ok {
		err := xerrors.Errorf("No accumulated state found for block %d", blockNumber)
		odrs.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, nil, err
	}

	currentBlock := &storage.Block{}
	err := odrs.DB.Where("number = ?", blockNumber).First(currentBlock).Error
	if err != nil {
		odrs.logger.Sugar().Errorw("Failed to fetch block", zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, nil, err
	}

	inserts := make([]*RewardSubmissionDiff, 0)
	for _, change := range accumulatedState {
		if change == nil {
			continue
		}

		inserts = append(inserts, &RewardSubmissionDiff{
			OperatorDirectedRewardSubmission: change,
			IsNew:                            true,
		})
	}

	// find all the records that are no longer active
	noLongerActiveSubmissions := make([]*OperatorDirectedRewardSubmission, 0)
	query := `
		select
			*
		from reward_submissions
		where
			block_number = @previousBlock
			and end_timestamp <= @blockTime
	`
	res := odrs.DB.
		Model(&OperatorDirectedRewardSubmission{}).
		Raw(query,
			sql.Named("previousBlock", blockNumber-1),
			sql.Named("blockTime", currentBlock.BlockTime),
		).
		Find(&noLongerActiveSubmissions)

	if res.Error != nil {
		odrs.logger.Sugar().Errorw("Failed to fetch no longer active submissions", zap.Error(res.Error))
		return nil, nil, res.Error
	}

	deletes := make([]*RewardSubmissionDiff, 0)
	for _, submission := range noLongerActiveSubmissions {
		deletes = append(deletes, &RewardSubmissionDiff{
			OperatorDirectedRewardSubmission: submission,
			IsNoLongerActive:                 true,
		})
	}
	return inserts, deletes, nil
}

// CommitFinalState commits the final state for the given block number.
func (odrs *OperatorDirectedRewardSubmissionsModel) CommitFinalState(blockNumber uint64) error {
	recordsToInsert, _, err := odrs.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(recordsToInsert) > 0 {
		for _, record := range recordsToInsert {
			res := odrs.DB.Model(&OperatorDirectedRewardSubmission{}).Clauses(clause.Returning{}).Create(&record.OperatorDirectedRewardSubmission)
			if res.Error != nil {
				odrs.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
				return res.Error
			}
		}
	}
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (odrs *OperatorDirectedRewardSubmissionsModel) GenerateStateRoot(blockNumber uint64) (types.StateRoot, error) {
	inserts, deletes, err := odrs.prepareState(blockNumber)
	if err != nil {
		return "", err
	}

	combinedResults := make([]*RewardSubmissionDiff, 0)
	combinedResults = append(combinedResults, inserts...)
	combinedResults = append(combinedResults, deletes...)

	inputs := odrs.sortValuesForMerkleTree(combinedResults)

	if len(inputs) == 0 {
		return "", nil
	}

	fullTree, err := odrs.MerkleizeState(blockNumber, inputs)
	if err != nil {
		odrs.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return "", err
	}
	return types.StateRoot(utils.ConvertBytesToString(fullTree.Root())), nil
}

func (odrs *OperatorDirectedRewardSubmissionsModel) sortValuesForMerkleTree(submissions []*RewardSubmissionDiff) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, submission := range submissions {
		slotID := NewSlotID(submission.OperatorDirectedRewardSubmission.RewardHash, submission.OperatorDirectedRewardSubmission.Strategy, submission.OperatorDirectedRewardSubmission.Operator)
		value := "added"
		if submission.IsNoLongerActive {
			value = "removed"
		}
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

func (odrs *OperatorDirectedRewardSubmissionsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return odrs.BaseEigenState.DeleteState("operator_directed_reward_submissions", startBlockNumber, endBlockNumber, odrs.DB)
}
