package operatorDirectedOperatorSetRewardSubmissions

import (
	"encoding/json"
	"fmt"
	"math/big"
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
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type OperatorDirectedOperatorSetRewardSubmission struct {
	Avs             string
	OperatorSetId   uint64
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
	Description     string
	BlockNumber     uint64
	TransactionHash string
	LogIndex        uint64
}

type OperatorDirectedOperatorSetRewardSubmissionsModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*OperatorDirectedOperatorSetRewardSubmission]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*OperatorDirectedOperatorSetRewardSubmission
	committedState   map[uint64][]*OperatorDirectedOperatorSetRewardSubmission
}

func NewOperatorDirectedOperatorSetRewardSubmissionsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*OperatorDirectedOperatorSetRewardSubmissionsModel, error) {
	model := &OperatorDirectedOperatorSetRewardSubmissionsModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*OperatorDirectedOperatorSetRewardSubmission),
		committedState:   make(map[uint64][]*OperatorDirectedOperatorSetRewardSubmission),
	}

	esm.RegisterState(model, 11)
	return model, nil
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) GetModelName() string {
	return "OperatorDirectedOperatorSetRewardSubmissionsModel"
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) NewSlotID(
	transactionHash string,
	logIndex uint64,
	rewardHash string,
	strategyIndex uint64,
	operatorIndex uint64,
) (types.SlotID, error) {
	return base.NewSlotIDWithSuffix(transactionHash, logIndex, fmt.Sprintf("%s_%016x_%016x", rewardHash, strategyIndex, operatorIndex)), nil
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

type OperatorSet struct {
	Avs string `json:"avs"`
	Id  uint64 `json:"id"`
}

type operatorDirectedRewardSubmissionOutputData struct {
	OperatorSet                       *OperatorSet                `json:"operatorSet"`
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

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) handleOperatorDirectedOperatorSetRewardSubmissionCreatedEvent(log *storage.TransactionLog) ([]*OperatorDirectedOperatorSetRewardSubmission, error) {
	arguments, err := od.ParseLogArguments(log)
	if err != nil {
		return nil, err
	}

	outputData, err := parseRewardSubmissionOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}
	outputRewardData := outputData.OperatorDirectedRewardsSubmission

	rewardSubmissions := make([]*OperatorDirectedOperatorSetRewardSubmission, 0)

	for i, strategyAndMultiplier := range outputRewardData.StrategiesAndMultipliers {
		startTimestamp := time.Unix(int64(outputRewardData.StartTimestamp), 0)
		endTimestamp := startTimestamp.Add(time.Duration(outputRewardData.Duration) * time.Second)

		multiplierBig, success := numbers.NewBig257().SetString(strategyAndMultiplier.Multiplier.String(), 10)
		if !success {
			return nil, fmt.Errorf("Failed to parse multiplier to Big257: %s", strategyAndMultiplier.Multiplier.String())
		}

		for j, operatorReward := range outputRewardData.OperatorRewards {
			amountBig, success := numbers.NewBig257().SetString(operatorReward.Amount.String(), 10)
			if !success {
				return nil, fmt.Errorf("Failed to parse amount to Big257: %s", operatorReward.Amount.String())
			}

			rewardSubmission := &OperatorDirectedOperatorSetRewardSubmission{
				Avs:             strings.ToLower(outputData.OperatorSet.Avs),
				OperatorSetId:   uint64(outputData.OperatorSet.Id),
				RewardHash:      strings.ToLower(arguments[1].Value.(string)),
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
				Description:     outputRewardData.Description,
				BlockNumber:     log.BlockNumber,
				TransactionHash: log.TransactionHash,
				LogIndex:        log.LogIndex,
			}

			rewardSubmissions = append(rewardSubmissions, rewardSubmission)
		}
	}

	return rewardSubmissions, nil
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) GetStateTransitions() (types.StateTransitions[[]*OperatorDirectedOperatorSetRewardSubmission], []uint64) {
	stateChanges := make(types.StateTransitions[[]*OperatorDirectedOperatorSetRewardSubmission])

	stateChanges[0] = func(log *storage.TransactionLog) ([]*OperatorDirectedOperatorSetRewardSubmission, error) {
		rewardSubmissions, err := od.handleOperatorDirectedOperatorSetRewardSubmissionCreatedEvent(log)
		if err != nil {
			return nil, err
		}

		for _, rewardSubmission := range rewardSubmissions {
			slotId, err := od.NewSlotID(
				rewardSubmission.TransactionHash,
				rewardSubmission.LogIndex,
				rewardSubmission.RewardHash,
				rewardSubmission.StrategyIndex,
				rewardSubmission.OperatorIndex,
			)
			if err != nil {
				od.logger.Sugar().Errorw("Failed to create slot ID",
					zap.String("transactionHash", log.TransactionHash),
					zap.Uint64("logIndex", log.LogIndex),
					zap.String("rewardHash", rewardSubmission.RewardHash),
					zap.Uint64("strategyIndex", rewardSubmission.StrategyIndex),
					zap.Uint64("operatorIndex", rewardSubmission.OperatorIndex),
					zap.Error(err),
				)
				return nil, err
			}

			_, ok := od.stateAccumulator[log.BlockNumber][slotId]
			if ok {
				err := fmt.Errorf("Duplicate operator directed operator set reward submission submitted for slot %s at block %d", slotId, log.BlockNumber)
				od.logger.Sugar().Errorw("Duplicate operator directed operator set reward submission submitted", zap.Error(err))
				return nil, err
			}

			od.stateAccumulator[log.BlockNumber][slotId] = rewardSubmission
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

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := od.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.RewardsCoordinator: {
			"OperatorDirectedOperatorSetRewardsSubmissionCreated",
		},
	}
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := od.getContractAddressesForEnvironment()
	return od.BaseEigenState.IsInterestingLog(addresses, log)
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) SetupStateForBlock(blockNumber uint64) error {
	od.stateAccumulator[blockNumber] = make(map[types.SlotID]*OperatorDirectedOperatorSetRewardSubmission)
	od.committedState[blockNumber] = make([]*OperatorDirectedOperatorSetRewardSubmission, 0)
	return nil
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(od.stateAccumulator, blockNumber)
	delete(od.committedState, blockNumber)
	return nil
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := od.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			od.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) prepareState(blockNumber uint64) ([]*OperatorDirectedOperatorSetRewardSubmission, error) {
	accumulatedState, ok := od.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		od.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*OperatorDirectedOperatorSetRewardSubmission, 0)
	for _, submission := range accumulatedState {
		recordsToInsert = append(recordsToInsert, submission)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) CommitFinalState(blockNumber uint64) error {
	recordsToInsert, err := od.prepareState(blockNumber)
	if err != nil {
		return err
	}

	if len(recordsToInsert) > 0 {
		for _, record := range recordsToInsert {
			res := od.DB.Model(&OperatorDirectedOperatorSetRewardSubmission{}).Clauses(clause.Returning{}).Create(&record)
			if res.Error != nil {
				od.logger.Sugar().Errorw("Failed to insert records", zap.Error(res.Error))
				return res.Error
			}
		}
	}
	od.committedState[blockNumber] = recordsToInsert
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := od.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := od.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := od.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		od.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := od.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		od.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) formatMerkleLeafValue(
	rewardHash string,
	strategy string,
	multiplier string,
	operator string,
	amount string,
) (string, error) {
	// Multiplier is a uint96 in the contracts, which translates to 24 hex characters
	// Amount is a uint256 in the contracts, which translates to 64 hex characters
	multiplierBig, success := new(big.Int).SetString(multiplier, 10)
	if !success {
		return "", fmt.Errorf("failed to parse multiplier to BigInt: %s", multiplier)
	}

	amountBig, success := new(big.Int).SetString(amount, 10)
	if !success {
		return "", fmt.Errorf("failed to parse amount to BigInt: %s", amount)
	}

	return fmt.Sprintf("%s_%s_%024x_%s_%064x", rewardHash, strategy, multiplierBig, operator, amountBig), nil
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) sortValuesForMerkleTree(submissions []*OperatorDirectedOperatorSetRewardSubmission) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, submission := range submissions {
		slotID, err := od.NewSlotID(
			submission.TransactionHash,
			submission.LogIndex,
			submission.RewardHash,
			submission.StrategyIndex,
			submission.OperatorIndex,
		)
		if err != nil {
			od.logger.Sugar().Errorw("Failed to create slot ID",
				zap.String("transactionHash", submission.TransactionHash),
				zap.Uint64("logIndex", submission.LogIndex),
				zap.String("rewardHash", submission.RewardHash),
				zap.Uint64("strategyIndex", submission.StrategyIndex),
				zap.Uint64("operatorIndex", submission.OperatorIndex),
				zap.Error(err),
			)
			return nil, err
		}

		value, err := od.formatMerkleLeafValue(
			submission.RewardHash,
			submission.Strategy,
			submission.Multiplier,
			submission.Operator,
			submission.Amount,
		)
		if err != nil {
			od.Logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.String("rewardHash", submission.RewardHash),
				zap.String("strategy", submission.Strategy),
				zap.String("multiplier", submission.Multiplier),
				zap.String("operator", submission.Operator),
				zap.String("amount", submission.Amount),
			)
			return nil, err
		}
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: slotID,
			Value:  []byte(value),
		})
	}

	slices.SortFunc(inputs, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})

	return inputs, nil
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return od.BaseEigenState.DeleteState("operator_directed_operator_set_reward_submissions", startBlockNumber, endBlockNumber, od.DB)
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	records := make([]*OperatorDirectedOperatorSetRewardSubmission, 0)
	res := od.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&records)
	if res.Error != nil {
		od.logger.Sugar().Errorw("Failed to list records for block range",
			zap.Error(res.Error),
			zap.Uint64("startBlockNumber", startBlockNumber),
			zap.Uint64("endBlockNumber", endBlockNumber),
		)
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (od *OperatorDirectedOperatorSetRewardSubmissionsModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
