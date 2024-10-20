package rewardSubmissions

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/base"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/types"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"github.com/Layr-Labs/go-sidecar/internal/types/numbers"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type RewardSubmission struct {
	Avs            string
	RewardHash     string
	Token          string
	Amount         string
	Strategy       string
	StrategyIndex  uint64
	Multiplier     string
	StartTimestamp *time.Time `gorm:"type:DATETIME"`
	EndTimestamp   *time.Time `gorm:"type:DATETIME"`
	Duration       uint64
	BlockNumber    uint64
	RewardType     string // avs, all_stakers, all_earners
}
type RewardSubmissions struct {
	Submissions []*RewardSubmission
}

func NewSlotID(rewardHash string, strategy string) types.SlotID {
	return types.SlotID(fmt.Sprintf("%s_%s", rewardHash, strategy))
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
}

func NewRewardSubmissionsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*RewardSubmissionsModel, error) {
	model := &RewardSubmissionsModel{
		BaseEigenState:   base.NewBaseEigenState(logger, grm),
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*RewardSubmission),
	}

	esm.RegisterState(model, 5)
	return model, nil
}

func (rs *RewardSubmissionsModel) GetModelName() string {
	return "RewardSubmissionsModel"
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

func (rs *RewardSubmissionsModel) handleRewardSubmissionCreatedEvent(log *storage.TransactionLog) (*RewardSubmissions, error) {
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

	rewardSubmissions := make([]*RewardSubmission, 0)

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

		rewardSubmission := &RewardSubmission{
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

	return &RewardSubmissions{Submissions: rewardSubmissions}, nil
}

func (rs *RewardSubmissionsModel) GetStateTransitions() types.StateTransitions {
	stateTransitions := make(types.StateTransitions)

	stateTransitions[0] = func(log *storage.TransactionLog) (interface{}, error) {
		rewardSubmissions, err := rs.handleRewardSubmissionCreatedEvent(log)
		if err != nil {
			return nil, err
		}

		for _, rewardSubmission := range rewardSubmissions.Submissions {
			slotId := NewSlotID(rewardSubmission.RewardHash, rewardSubmission.Strategy)

			_, ok := rs.stateAccumulator[log.BlockNumber][slotId]
			if ok {
				err := xerrors.Errorf("Duplicate distribution root submitted for slot %s at block %d", slotId, log.BlockNumber)
				rs.logger.Sugar().Errorw("Duplicate distribution root submitted", zap.Error(err))
				return nil, err
			}

			rs.stateAccumulator[log.BlockNumber][slotId] = rewardSubmission
		}

		return rewardSubmissions, nil
	}

	return stateTransitions
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
	return nil
}

func (rs *RewardSubmissionsModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(rs.stateAccumulator, blockNumber)
	return nil
}

func (rs *RewardSubmissionsModel) HandleLog(log *storage.TransactionLog) (interface{}, error) {
	stateTransitions := rs.GetStateTransitions()
	return rs.BaseEigenState.HandleLog(stateTransitions, log)
}

func (rs *RewardSubmissionsModel) clonePreviousBlocksToNewBlock(blockNumber uint64) error {
	query := `
		insert into reward_submissions(avs, reward_hash, token, amount, strategy, strategy_index, multiplier, start_timestamp, end_timestamp, duration, reward_type, block_number)
			select
				avs,
				reward_hash,
				token,
				amount,
				strategy,
				strategy_index,
				multiplier,
				start_timestamp,
				end_timestamp,
				duration,
				reward_type,
				@currentBlock as block_number
			from reward_submissions
			where block_number = @previousBlock
	`
	res := rs.DB.Exec(query,
		sql.Named("currentBlock", blockNumber),
		sql.Named("previousBlock", blockNumber-1),
	)

	if res.Error != nil {
		rs.logger.Sugar().Errorw("Failed to clone previous block state to new block", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

// prepareState prepares the state for commit by adding the new state to the existing state.
func (rs *RewardSubmissionsModel) prepareState(blockNumber uint64) ([]*RewardSubmission, []*RewardSubmission, error) {
	accumulatedState, ok := rs.stateAccumulator[blockNumber]
	if !ok {
		err := xerrors.Errorf("No accumulated state found for block %d", blockNumber)
		rs.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, nil, err
	}

	currentBlock := &storage.Block{}
	err := rs.DB.Where("number = ?", blockNumber).First(currentBlock).Error
	if err != nil {
		rs.logger.Sugar().Errorw("Failed to fetch block", zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, nil, err
	}

	inserts := make([]*RewardSubmission, 0)
	for _, change := range accumulatedState {
		if change == nil {
			continue
		}

		inserts = append(inserts, change)
	}

	// find all the records that are no longer active and delete them
	deletes := make([]*RewardSubmission, 0)
	query := `
		select
			*
		from reward_submissions
		where
			block_number = @previousBlock
			and end_timestamp <= @blockTime
	`
	res := rs.DB.
		Model(&RewardSubmission{}).
		Raw(query,
			sql.Named("previousBlock", blockNumber-1),
			sql.Named("blockTime", currentBlock.BlockTime.Unix()),
		).
		Find(&deletes)

	if res.Error != nil {
		rs.logger.Sugar().Errorw("Failed to fetch no longer active submissions", zap.Error(res.Error))
		return nil, nil, res.Error
	}
	return inserts, deletes, nil
}

// CommitFinalState commits the final state for the given block number.
func (rs *RewardSubmissionsModel) CommitFinalState(blockNumber uint64) error {
	err := rs.clonePreviousBlocksToNewBlock(blockNumber)
	if err != nil {
		return err
	}

	recordsToInsert, recordsToDelete, err := rs.prepareState(blockNumber)
	if err != nil {
		return err
	}

	for _, record := range recordsToDelete {
		res := rs.DB.Delete(&RewardSubmission{}, "reward_hash = ? and strategy = ? and block_number = ?", record.RewardHash, record.Strategy, blockNumber)
		if res.Error != nil {
			rs.logger.Sugar().Errorw("Failed to delete record",
				zap.Error(res.Error),
				zap.String("rewardHash", record.RewardHash),
				zap.String("strategy", record.Strategy),
				zap.Uint64("blockNumber", blockNumber),
			)
			return res.Error
		}
	}
	if len(recordsToInsert) > 0 {
		res := rs.DB.Model(&RewardSubmission{}).Clauses(clause.Returning{}).Create(&recordsToInsert)
		if res.Error != nil {
			rs.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
			return res.Error
		}
	}
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (rs *RewardSubmissionsModel) GenerateStateRoot(blockNumber uint64) (types.StateRoot, error) {
	inserts, deletes, err := rs.prepareState(blockNumber)
	if err != nil {
		return "", err
	}

	stateDiffs := make([]*base.StateDiff, 0)
	for _, record := range inserts {
		stateDiffs = append(stateDiffs, &base.StateDiff{
			SlotID: NewSlotID(record.RewardHash, record.Strategy),
			Value:  []byte("added"),
		})
	}
	for _, record := range deletes {
		stateDiffs = append(stateDiffs, &base.StateDiff{
			SlotID: NewSlotID(record.RewardHash, record.Strategy),
			Value:  []byte("removed"),
		})
	}

	return rs.BaseEigenState.MerkleizeState(blockNumber, stateDiffs)
}

func (rs *RewardSubmissionsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return rs.BaseEigenState.DeleteState("reward_submissions", startBlockNumber, endBlockNumber)
}
