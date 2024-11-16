package operatorDirectedRewardSubmissions

import (
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/types/numbers"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type OperatorDirectedRewardSubmission struct {
	Avs             string
	RewardHash      string
	Token           string
	Operator        string
	OperatorIndex   uint64
	Amount          string
	Strategy        string
	StrategyIndex   uint64
	Multiplier      string
	StartTimestamp  *time.Time
	EndTimestamp    *time.Time
	Duration        uint64
	BlockNumber     uint64
	TransactionHash string
	LogIndex        uint64
}

func NewSlotID(transactionHash string, logIndex uint64, rewardHash string, strategyIndex uint64, operatorIndex uint64) types.SlotID {
	return base.NewSlotIDWithSuffix(transactionHash, logIndex, fmt.Sprintf("%s_%d_%d", rewardHash, strategyIndex, operatorIndex))
}

type OperatorDirectedRewardSubmissionsModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*OperatorDirectedRewardSubmission]
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

type operatorDirectedRewardData struct {
	StrategiesAndMultipliers []struct {
		Strategy   string      `json:"strategy"`
		Multiplier json.Number `json:"multiplier"`
	} `json:"strategiesAndMultipliers"`
	Token           string `json:"token"`
	OperatorRewards []struct {
		Operator string      `json:"operator"`
		Amount   json.Number `json:"amount"`
	} `json:"operatorRewards"`
	StartTimestamp uint64 `json:"startTimestamp"`
	Duration       uint64 `json:"duration"`
	Description    string `json:"description"`
}

type operatorDirectedRewardSubmissionOutputData struct {
	SubmissionNonce                   json.Number                 `json:"submissionNonce"`
	OperatorDirectedRewardsSubmission *operatorDirectedRewardData `json:"operatorDirectedRewardsSubmission"`
}

func parseRewardSubmissionOutputData(outputDataStr string) (*operatorDirectedRewardSubmissionOutputData, error) {
	outputData := &operatorDirectedRewardSubmissionOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (odrs *OperatorDirectedRewardSubmissionsModel) handleOperatorDirectedRewardSubmissionCreatedEvent(log *storage.TransactionLog) ([]*OperatorDirectedRewardSubmission, error) {
	arguments, err := odrs.ParseLogArguments(log)
	if err != nil {
		return nil, err
	}

	outputData, err := parseRewardSubmissionOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}
	outputRewardData := outputData.OperatorDirectedRewardsSubmission

	rewardSubmissions := make([]*OperatorDirectedRewardSubmission, 0)

	for i, strategyAndMultiplier := range outputRewardData.StrategiesAndMultipliers {
		startTimestamp := time.Unix(int64(outputRewardData.StartTimestamp), 0)
		endTimestamp := startTimestamp.Add(time.Duration(outputRewardData.Duration) * time.Second)

		multiplierBig, success := numbers.NewBig257().SetString(strategyAndMultiplier.Multiplier.String(), 10)
		if !success {
			return nil, xerrors.Errorf("Failed to parse multiplier to Big257: %s", strategyAndMultiplier.Multiplier.String())
		}

		for j, operatorReward := range outputRewardData.OperatorRewards {
			amountBig, success := numbers.NewBig257().SetString(operatorReward.Amount.String(), 10)
			if !success {
				return nil, xerrors.Errorf("Failed to parse amount to Big257: %s", operatorReward.Amount.String())
			}

			rewardSubmission := &OperatorDirectedRewardSubmission{
				Avs:             strings.ToLower(arguments[1].Value.(string)),
				RewardHash:      strings.ToLower(arguments[2].Value.(string)),
				Token:           strings.ToLower(outputRewardData.Token),
				Operator:        strings.ToLower(operatorReward.Operator),
				OperatorIndex:   uint64(j),
				Amount:          amountBig.String(),
				Strategy:        strings.ToLower(strategyAndMultiplier.Strategy),
				StrategyIndex:   uint64(i),
				Multiplier:      multiplierBig.String(),
				StartTimestamp:  &startTimestamp,
				EndTimestamp:    &endTimestamp,
				Duration:        outputRewardData.Duration,
				BlockNumber:     log.BlockNumber,
				TransactionHash: log.TransactionHash,
				LogIndex:        log.LogIndex,
			}

			rewardSubmissions = append(rewardSubmissions, rewardSubmission)
		}
	}

	return rewardSubmissions, nil
}

func (odrs *OperatorDirectedRewardSubmissionsModel) GetStateTransitions() (types.StateTransitions[[]*OperatorDirectedRewardSubmission], []uint64) {
	stateChanges := make(types.StateTransitions[[]*OperatorDirectedRewardSubmission])

	stateChanges[0] = func(log *storage.TransactionLog) ([]*OperatorDirectedRewardSubmission, error) {
		rewardSubmissions, err := odrs.handleOperatorDirectedRewardSubmissionCreatedEvent(log)
		if err != nil {
			return nil, err
		}

		for _, rewardSubmission := range rewardSubmissions {
			slotId := NewSlotID(rewardSubmission.TransactionHash, rewardSubmission.LogIndex, rewardSubmission.RewardHash, rewardSubmission.StrategyIndex, rewardSubmission.OperatorIndex)

			_, ok := odrs.stateAccumulator[log.BlockNumber][slotId]
			if ok {
				fmt.Printf("Submissions: %+v\n", odrs.stateAccumulator[log.BlockNumber])
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
func (odrs *OperatorDirectedRewardSubmissionsModel) prepareState(blockNumber uint64) ([]*OperatorDirectedRewardSubmission, error) {
	accumulatedState, ok := odrs.stateAccumulator[blockNumber]
	if !ok {
		err := xerrors.Errorf("No accumulated state found for block %d", blockNumber)
		odrs.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*OperatorDirectedRewardSubmission, 0)
	for _, submission := range accumulatedState {
		recordsToInsert = append(recordsToInsert, submission)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (odrs *OperatorDirectedRewardSubmissionsModel) CommitFinalState(blockNumber uint64) error {
	recordsToInsert, err := odrs.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(recordsToInsert) > 0 {
		for _, record := range recordsToInsert {
			res := odrs.DB.Model(&OperatorDirectedRewardSubmission{}).Clauses(clause.Returning{}).Create(&record)
			if res.Error != nil {
				odrs.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
				return res.Error
			}
		}
	}
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (odrs *OperatorDirectedRewardSubmissionsModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := odrs.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs := odrs.sortValuesForMerkleTree(inserts)

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := odrs.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		odrs.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (odrs *OperatorDirectedRewardSubmissionsModel) sortValuesForMerkleTree(submissions []*OperatorDirectedRewardSubmission) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, submission := range submissions {
		slotID := NewSlotID(submission.TransactionHash, submission.LogIndex, submission.RewardHash, submission.StrategyIndex, submission.OperatorIndex)
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

func (odrs *OperatorDirectedRewardSubmissionsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return odrs.BaseEigenState.DeleteState("operator_directed_reward_submissions", startBlockNumber, endBlockNumber, odrs.DB)
}
