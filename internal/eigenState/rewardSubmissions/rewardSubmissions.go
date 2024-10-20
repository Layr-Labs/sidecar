package rewardSubmissions

import (
	"database/sql"
	"encoding/json"
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

type RewardSubmissionDiff struct {
	RewardSubmission *RewardSubmission
	IsNew            bool
	IsNoLongerActive bool
}

type RewardSubmissions struct {
	Submissions []*RewardSubmission
}

func NewSlotID(rewardHash string, strategy string) types.SlotID {
	return types.SlotID(fmt.Sprintf("%s_%s", rewardHash, strategy))
}

type RewardSubmissionsBaseModel struct {
	StateTransitions types.StateTransitions[RewardSubmission]
	db               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*RewardSubmission
}

func NewRewardSubmissionsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*eigenStateModel.EigenStateModel, error) {
	base := &RewardSubmissionsBaseModel{
		db:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*RewardSubmission),
	}
	m := eigenStateModel.NewEigenStateModel(base)

	esm.RegisterState(m, 5)
	return m, nil
}

func (rs *RewardSubmissionsBaseModel) Logger() *zap.Logger {
	return rs.logger
}

func (rs *RewardSubmissionsBaseModel) ModelName() string {
	return "RewardSubmissionsBaseModel"
}

func (rs *RewardSubmissionsBaseModel) TableName() string {
	return "reward_submissions"
}

func (rs *RewardSubmissionsBaseModel) DB() *gorm.DB {
	return rs.db
}

func (rs *RewardSubmissionsBaseModel) Base() interface{} {
	return rs
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

func (rs *RewardSubmissionsBaseModel) handleRewardSubmissionCreatedEvent(log *storage.TransactionLog) (*RewardSubmissions, error) {
	arguments, err := utils.ParseLogArguments(rs.logger, log)
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

func (rs *RewardSubmissionsBaseModel) GetStateTransitions() (types.StateTransitions[RewardSubmissions], []uint64) {
	stateChanges := make(types.StateTransitions[RewardSubmissions])

	stateChanges[0] = func(log *storage.TransactionLog) (*RewardSubmissions, error) {
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

func (rs *RewardSubmissionsBaseModel) GetInterestingLogMap() map[string][]string {
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

func (rs *RewardSubmissionsBaseModel) InitBlock(blockNumber uint64) error {
	rs.stateAccumulator[blockNumber] = make(map[types.SlotID]*RewardSubmission)
	return nil
}

func (rs *RewardSubmissionsBaseModel) CleanupBlock(blockNumber uint64) error {
	delete(rs.stateAccumulator, blockNumber)
	return nil
}

func (rs *RewardSubmissionsBaseModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := rs.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			rs.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", blockNumber))

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

func (rs *RewardSubmissionsBaseModel) clonePreviousBlocksToNewBlock(blockNumber uint64) error {
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
	res := rs.db.Exec(query,
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
func (rs *RewardSubmissionsBaseModel) prepareState(blockNumber uint64) ([]*RewardSubmissionDiff, []*RewardSubmissionDiff, error) {
	accumulatedState, ok := rs.stateAccumulator[blockNumber]
	if !ok {
		err := xerrors.Errorf("No accumulated state found for block %d", blockNumber)
		rs.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, nil, err
	}

	currentBlock := &storage.Block{}
	err := rs.db.Where("number = ?", blockNumber).First(currentBlock).Error
	if err != nil {
		rs.logger.Sugar().Errorw("Failed to fetch block", zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, nil, err
	}

	inserts := make([]*RewardSubmissionDiff, 0)
	for _, change := range accumulatedState {
		if change == nil {
			continue
		}

		inserts = append(inserts, &RewardSubmissionDiff{
			RewardSubmission: change,
			IsNew:            true,
		})
	}

	// find all the records that are no longer active
	noLongerActiveSubmissions := make([]*RewardSubmission, 0)
	query := `
		select
			*
		from reward_submissions
		where
			block_number = @previousBlock
			and end_timestamp <= @blockTime
	`
	res := rs.db.
		Model(&RewardSubmission{}).
		Raw(query,
			sql.Named("previousBlock", blockNumber-1),
			sql.Named("blockTime", currentBlock.BlockTime.Unix()),
		).
		Find(&noLongerActiveSubmissions)

	if res.Error != nil {
		rs.logger.Sugar().Errorw("Failed to fetch no longer active submissions", zap.Error(res.Error))
		return nil, nil, res.Error
	}

	deletes := make([]*RewardSubmissionDiff, 0)
	for _, submission := range noLongerActiveSubmissions {
		deletes = append(deletes, &RewardSubmissionDiff{
			RewardSubmission: submission,
			IsNoLongerActive: true,
		})
	}
	return inserts, deletes, nil
}

// CommitFinalState commits the final state for the given block number.
func (rs *RewardSubmissionsBaseModel) CommitFinalState(blockNumber uint64) error {
	err := rs.clonePreviousBlocksToNewBlock(blockNumber)
	if err != nil {
		return err
	}

	recordsToInsert, recordsToDelete, err := rs.prepareState(blockNumber)
	if err != nil {
		return err
	}

	for _, record := range recordsToDelete {
		res := rs.db.Delete(&RewardSubmission{}, "reward_hash = ? and strategy = ? and block_number = ?", record.RewardSubmission.RewardHash, record.RewardSubmission.Strategy, blockNumber)
		if res.Error != nil {
			rs.logger.Sugar().Errorw("Failed to delete record",
				zap.Error(res.Error),
				zap.String("rewardHash", record.RewardSubmission.RewardHash),
				zap.String("strategy", record.RewardSubmission.Strategy),
				zap.Uint64("blockNumber", blockNumber),
			)
			return res.Error
		}
	}
	if len(recordsToInsert) > 0 {
		// records := make([]RewardSubmission, 0)
		for _, record := range recordsToInsert {
			res := rs.db.Model(&RewardSubmission{}).Clauses(clause.Returning{}).Create(&record.RewardSubmission)
			if res.Error != nil {
				rs.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
				fmt.Printf("\n\n%+v\n\n", record.RewardSubmission)
				return res.Error
			}
		}
	}
	return nil
}

func (rs *RewardSubmissionsBaseModel) GetStateDiffs(blockNumber uint64) ([]types.StateDiff, error) {
	inserts, deletes, err := rs.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	diffs := make([]*RewardSubmissionDiff, 0)
	diffs = append(diffs, inserts...)
	diffs = append(diffs, deletes...)

	stateDiffs := make([]types.StateDiff, 0)
	for _, diff := range diffs {
		slotID := NewSlotID(diff.RewardSubmission.RewardHash, diff.RewardSubmission.Strategy)
		value := "added"
		if diff.IsNoLongerActive {
			value = "removed"
		}
		stateDiffs = append(stateDiffs, types.StateDiff{
			SlotID: slotID,
			Value:  []byte(value),
		})
	}

	return stateDiffs, nil
}
