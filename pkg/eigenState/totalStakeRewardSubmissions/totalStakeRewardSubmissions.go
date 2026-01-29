package totalStakeRewardSubmissions

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
)

// TotalStakeRewardSubmission represents a reward submission for total stake rewards.
// Unlike operator-directed rewards, these rewards have a single amount for the entire operator set
// that gets distributed pro-rata across operators based on their total delegated stake.
type TotalStakeRewardSubmission struct {
	Avs             string
	OperatorSetId   uint64
	RewardHash      string
	Token           string
	Amount          string // Total amount (not per-operator)
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

type TotalStakeRewardSubmissionsModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[[]*TotalStakeRewardSubmission]
	DB               *gorm.DB
	Network          config.Network
	Environment      config.Environment
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*TotalStakeRewardSubmission
	committedState   map[uint64][]*TotalStakeRewardSubmission
}

func NewTotalStakeRewardSubmissionsModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*TotalStakeRewardSubmissionsModel, error) {
	model := &TotalStakeRewardSubmissionsModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64]map[types.SlotID]*TotalStakeRewardSubmission),
		committedState:   make(map[uint64][]*TotalStakeRewardSubmission),
	}

	esm.RegisterState(model, 25)
	return model, nil
}

const TotalStakeRewardSubmissionsModelName = "TotalStakeRewardSubmissionsModel"

func (ts *TotalStakeRewardSubmissionsModel) GetModelName() string {
	return TotalStakeRewardSubmissionsModelName
}

func (ts *TotalStakeRewardSubmissionsModel) NewSlotID(
	transactionHash string,
	logIndex uint64,
	rewardHash string,
	strategyIndex uint64,
) (types.SlotID, error) {
	return base.NewSlotIDWithSuffix(transactionHash, logIndex, fmt.Sprintf("%s_%016x", rewardHash, strategyIndex)), nil
}

// stakeRewardData represents the rewardsSubmission structure for stake-based rewards
type stakeRewardData struct {
	StrategiesAndMultipliers []struct {
		Strategy   string      `json:"strategy"`
		Multiplier json.Number `json:"multiplier"`
	} `json:"strategiesAndMultipliers"`
	Token          string      `json:"token"`
	Amount         json.Number `json:"amount"` // Total amount
	StartTimestamp uint64      `json:"startTimestamp"`
	Duration       uint64      `json:"duration"`
}

type OperatorSet struct {
	Avs string `json:"avs"`
	Id  uint64 `json:"id"`
}

type stakeRewardSubmissionOutputData struct {
	OperatorSet       *OperatorSet     `json:"operatorSet"`
	SubmissionNonce   json.Number      `json:"submissionNonce"`
	RewardsSubmission *stakeRewardData `json:"rewardsSubmission"`
}

func parseStakeRewardSubmissionOutputData(outputDataStr string) (*stakeRewardSubmissionOutputData, error) {
	outputData := &stakeRewardSubmissionOutputData{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}

	return outputData, err
}

func (ts *TotalStakeRewardSubmissionsModel) handleTotalStakeRewardsSubmissionCreatedEvent(log *storage.TransactionLog) ([]*TotalStakeRewardSubmission, error) {
	arguments, err := ts.ParseLogArguments(log)
	if err != nil {
		return nil, err
	}

	outputData, err := parseStakeRewardSubmissionOutputData(log.OutputData)
	if err != nil {
		return nil, err
	}
	outputRewardData := outputData.RewardsSubmission

	if outputRewardData.Duration == 0 {
		ts.Logger.Sugar().Debugw("Skipping total stake reward submission with zero duration",
			zap.String("transactionHash", log.TransactionHash),
			zap.Uint64("logIndex", log.LogIndex),
			zap.Uint64("blockNumber", log.BlockNumber),
		)
		return []*TotalStakeRewardSubmission{}, nil
	}

	rewardSubmissions := make([]*TotalStakeRewardSubmission, 0)

	amountBig, success := numbers.NewBig257().SetString(outputRewardData.Amount.String(), 10)
	if !success {
		return nil, fmt.Errorf("Failed to parse amount to Big257: %s", outputRewardData.Amount.String())
	}

	for i, strategyAndMultiplier := range outputRewardData.StrategiesAndMultipliers {
		startTimestamp := time.Unix(int64(outputRewardData.StartTimestamp), 0)
		endTimestamp := startTimestamp.Add(time.Duration(outputRewardData.Duration) * time.Second)

		multiplierBig, success := numbers.NewBig257().SetString(strategyAndMultiplier.Multiplier.String(), 10)
		if !success {
			return nil, fmt.Errorf("Failed to parse multiplier to Big257: %s", strategyAndMultiplier.Multiplier.String())
		}

		rewardSubmission := &TotalStakeRewardSubmission{
			Avs:             strings.ToLower(outputData.OperatorSet.Avs),
			OperatorSetId:   uint64(outputData.OperatorSet.Id),
			RewardHash:      strings.ToLower(arguments[1].Value.(string)),
			Token:           strings.ToLower(outputRewardData.Token),
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

	return rewardSubmissions, nil
}

func (ts *TotalStakeRewardSubmissionsModel) GetStateTransitions() (types.StateTransitions[[]*TotalStakeRewardSubmission], []uint64) {
	stateChanges := make(types.StateTransitions[[]*TotalStakeRewardSubmission])

	stateChanges[0] = func(log *storage.TransactionLog) ([]*TotalStakeRewardSubmission, error) {
		rewardSubmissions, err := ts.handleTotalStakeRewardsSubmissionCreatedEvent(log)
		if err != nil {
			return nil, err
		}

		for _, rewardSubmission := range rewardSubmissions {
			slotId, err := ts.NewSlotID(
				rewardSubmission.TransactionHash,
				rewardSubmission.LogIndex,
				rewardSubmission.RewardHash,
				rewardSubmission.StrategyIndex,
			)
			if err != nil {
				ts.logger.Sugar().Errorw("Failed to create slot ID",
					zap.String("transactionHash", log.TransactionHash),
					zap.Uint64("logIndex", log.LogIndex),
					zap.String("rewardHash", rewardSubmission.RewardHash),
					zap.Uint64("strategyIndex", rewardSubmission.StrategyIndex),
					zap.Error(err),
				)
				return nil, err
			}

			_, ok := ts.stateAccumulator[log.BlockNumber][slotId]
			if ok {
				err := fmt.Errorf("Duplicate total stake reward submission submitted for slot %s at block %d", slotId, log.BlockNumber)
				ts.logger.Sugar().Errorw("Duplicate total stake reward submission submitted", zap.Error(err))
				return nil, err
			}

			ts.stateAccumulator[log.BlockNumber][slotId] = rewardSubmission
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

func (ts *TotalStakeRewardSubmissionsModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := ts.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.RewardsCoordinator: {
			"TotalStakeRewardsSubmissionCreated",
		},
	}
}

func (ts *TotalStakeRewardSubmissionsModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := ts.getContractAddressesForEnvironment()
	return ts.BaseEigenState.IsInterestingLog(addresses, log)
}

func (ts *TotalStakeRewardSubmissionsModel) SetupStateForBlock(blockNumber uint64) error {
	ts.stateAccumulator[blockNumber] = make(map[types.SlotID]*TotalStakeRewardSubmission)
	ts.committedState[blockNumber] = make([]*TotalStakeRewardSubmission, 0)
	return nil
}

func (ts *TotalStakeRewardSubmissionsModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(ts.stateAccumulator, blockNumber)
	delete(ts.committedState, blockNumber)
	return nil
}

func (ts *TotalStakeRewardSubmissionsModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := ts.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			ts.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

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
func (ts *TotalStakeRewardSubmissionsModel) prepareState(blockNumber uint64) ([]*TotalStakeRewardSubmission, error) {
	accumulatedState, ok := ts.stateAccumulator[blockNumber]
	if !ok {
		err := fmt.Errorf("No accumulated state found for block %d", blockNumber)
		ts.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	recordsToInsert := make([]*TotalStakeRewardSubmission, 0)
	for _, submission := range accumulatedState {
		recordsToInsert = append(recordsToInsert, submission)
	}
	return recordsToInsert, nil
}

// CommitFinalState commits the final state for the given block number.
func (ts *TotalStakeRewardSubmissionsModel) CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error {
	recordsToInsert, err := ts.prepareState(blockNumber)
	if err != nil {
		return err
	}

	insertedRecords, err := base.CommitFinalState(recordsToInsert, ignoreInsertConflicts, ts.GetTableName(), ts.DB)
	if err != nil {
		ts.logger.Sugar().Errorw("Failed to insert records", zap.Error(err))
		return err
	}
	ts.committedState[blockNumber] = insertedRecords
	return nil
}

// GenerateStateRoot generates the state root for the given block number using the results of the state changes.
func (ts *TotalStakeRewardSubmissionsModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	inserts, err := ts.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs, err := ts.sortValuesForMerkleTree(inserts)
	if err != nil {
		return nil, err
	}

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := ts.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		ts.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (ts *TotalStakeRewardSubmissionsModel) GetCommittedState(blockNumber uint64) ([]interface{}, error) {
	records, ok := ts.committedState[blockNumber]
	if !ok {
		err := fmt.Errorf("No committed state found for block %d", blockNumber)
		ts.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (ts *TotalStakeRewardSubmissionsModel) formatMerkleLeafValue(
	rewardHash string,
	strategy string,
	multiplier string,
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

	return fmt.Sprintf("%s_%s_%024x_%064x", rewardHash, strategy, multiplierBig, amountBig), nil
}

func (ts *TotalStakeRewardSubmissionsModel) sortValuesForMerkleTree(submissions []*TotalStakeRewardSubmission) ([]*base.MerkleTreeInput, error) {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, submission := range submissions {
		slotID, err := ts.NewSlotID(
			submission.TransactionHash,
			submission.LogIndex,
			submission.RewardHash,
			submission.StrategyIndex,
		)
		if err != nil {
			ts.logger.Sugar().Errorw("Failed to create slot ID",
				zap.String("transactionHash", submission.TransactionHash),
				zap.Uint64("logIndex", submission.LogIndex),
				zap.String("rewardHash", submission.RewardHash),
				zap.Uint64("strategyIndex", submission.StrategyIndex),
				zap.Error(err),
			)
			return nil, err
		}

		value, err := ts.formatMerkleLeafValue(
			submission.RewardHash,
			submission.Strategy,
			submission.Multiplier,
			submission.Amount,
		)
		if err != nil {
			ts.Logger.Sugar().Errorw("Failed to format merkle leaf value",
				zap.Error(err),
				zap.String("rewardHash", submission.RewardHash),
				zap.String("strategy", submission.Strategy),
				zap.String("multiplier", submission.Multiplier),
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

func (ts *TotalStakeRewardSubmissionsModel) GetTableName() string {
	return "total_stake_reward_submissions"
}

func (ts *TotalStakeRewardSubmissionsModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return ts.BaseEigenState.DeleteState(ts.GetTableName(), startBlockNumber, endBlockNumber, ts.DB)
}

func (ts *TotalStakeRewardSubmissionsModel) ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error) {
	records := make([]*TotalStakeRewardSubmission, 0)
	res := ts.DB.Where("block_number >= ? AND block_number <= ?", startBlockNumber, endBlockNumber).Find(&records)
	if res.Error != nil {
		ts.logger.Sugar().Errorw("Failed to list records for block range",
			zap.Error(res.Error),
			zap.Uint64("startBlockNumber", startBlockNumber),
			zap.Uint64("endBlockNumber", endBlockNumber),
		)
		return nil, res.Error
	}
	return base.CastCommittedStateToInterface(records), nil
}

func (ts *TotalStakeRewardSubmissionsModel) IsActiveForBlockHeight(blockHeight uint64) (bool, error) {
	return true, nil
}
