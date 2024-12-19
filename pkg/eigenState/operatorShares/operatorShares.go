package operatorShares

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/shopspring/decimal"
	"math/big"
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
	"gorm.io/gorm/clause"
)

// OperatorShares represents the state of an operator's shares in a strategy at a given block number.
type OperatorShares struct {
	Operator    string
	Strategy    string
	Shares      string
	BlockNumber uint64
	CreatedAt   time.Time
}

// AccumulatedStateChange represents the accumulated state change for an operator's shares in a strategy at a given block number.
type AccumulatedStateChange struct {
	Operator    string
	Strategy    string
	Shares      *big.Int
	BlockNumber uint64
}

type OperatorSharesDiff struct {
	Operator    string
	Strategy    string
	Shares      *big.Int
	BlockNumber uint64
	IsNew       bool
}

type OperatorShareDeltas struct {
	Operator        string
	Staker          string
	Strategy        string
	Shares          string
	TransactionHash string
	LogIndex        uint64
	BlockNumber     uint64
	BlockTime       time.Time
	BlockDate       string
}

func NewSlotID(operator string, strategy string, staker string, transactionHash string, logIndex uint64) types.SlotID {
	return base.NewSlotIDWithSuffix(transactionHash, logIndex, fmt.Sprintf("%s_%s_%s", operator, strategy, staker))
}

// Implements IEigenStateModel.
type OperatorSharesModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[AccumulatedStateChange]
	DB               *gorm.DB
	logger           *zap.Logger
	globalConfig     *config.Config

	stateAccumulator map[uint64][]*OperatorShareDeltas
}

func NewOperatorSharesModel(
	esm *stateManager.EigenStateManager,
	grm *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) (*OperatorSharesModel, error) {
	model := &OperatorSharesModel{
		BaseEigenState: base.BaseEigenState{
			Logger: logger,
		},
		DB:               grm,
		logger:           logger,
		globalConfig:     globalConfig,
		stateAccumulator: make(map[uint64][]*OperatorShareDeltas),
	}

	esm.RegisterState(model, 1)
	return model, nil
}

func (osm *OperatorSharesModel) GetModelName() string {
	return "OperatorSharesModel"
}

type operatorSharesOutput struct {
	Strategy string      `json:"strategy"`
	Shares   json.Number `json:"shares"`
	Staker   string      `json:"staker"`
}

func parseLogOutputForOperatorShares(outputDataStr string) (*operatorSharesOutput, error) {
	outputData := &operatorSharesOutput{}
	decoder := json.NewDecoder(strings.NewReader(outputDataStr))
	decoder.UseNumber()

	err := decoder.Decode(&outputData)
	if err != nil {
		return nil, err
	}
	outputData.Strategy = strings.ToLower(outputData.Strategy)
	return outputData, err
}

func (osm *OperatorSharesModel) GetStateTransitions() (types.StateTransitions[*OperatorShareDeltas], []uint64) {
	stateChanges := make(types.StateTransitions[*OperatorShareDeltas])

	stateChanges[0] = func(log *storage.TransactionLog) (*OperatorShareDeltas, error) {
		arguments, err := osm.ParseLogArguments(log)
		if err != nil {
			return nil, err
		}
		outputData, err := parseLogOutputForOperatorShares(log.OutputData)
		if err != nil {
			return nil, err
		}

		// Sanity check to make sure we've got an initialized accumulator map for the block
		if _, ok := osm.stateAccumulator[log.BlockNumber]; !ok {
			return nil, fmt.Errorf("No state accumulator found for block %d", log.BlockNumber)
		}
		operator := strings.ToLower(arguments[0].Value.(string))

		sharesStr := outputData.Shares.String()
		shares, err := decimal.NewFromString(sharesStr)
		if err != nil {
			osm.logger.Sugar().Errorw("Failed to convert shares to big.Int",
				zap.String("shares", sharesStr),
				zap.String("transactionHash", log.TransactionHash),
				zap.Uint64("transactionIndex", log.TransactionIndex),
				zap.Uint64("blockNumber", log.BlockNumber),
			)
			return nil, fmt.Errorf("Failed to convert shares to big.Int: %s", sharesStr)
		}

		// All shares are emitted as ABS(shares), so we need to negate the shares if the event is a decrease
		if log.EventName == "OperatorSharesDecreased" {
			shares = shares.Mul(decimal.NewFromInt(-1))
		}

		delta := &OperatorShareDeltas{
			Operator:        operator,
			Strategy:        strings.ToLower(outputData.Strategy),
			Staker:          strings.ToLower(outputData.Staker),
			Shares:          shares.String(),
			TransactionHash: log.TransactionHash,
			LogIndex:        log.LogIndex,
			BlockNumber:     log.BlockNumber,
		}
		osm.stateAccumulator[log.BlockNumber] = append(osm.stateAccumulator[log.BlockNumber], delta)
		return delta, nil
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

func (osm *OperatorSharesModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := osm.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.DelegationManager: {
			"OperatorSharesIncreased",
			"OperatorSharesDecreased",
		},
	}
}

func (osm *OperatorSharesModel) IsInterestingLog(log *storage.TransactionLog) bool {
	addresses := osm.getContractAddressesForEnvironment()
	return osm.BaseEigenState.IsInterestingLog(addresses, log)
}

func (osm *OperatorSharesModel) SetupStateForBlock(blockNumber uint64) error {
	osm.stateAccumulator[blockNumber] = make([]*OperatorShareDeltas, 0)
	return nil
}

func (osm *OperatorSharesModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(osm.stateAccumulator, blockNumber)
	return nil
}

func (osm *OperatorSharesModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := osm.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			osm.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", log.BlockNumber))

			change, err := stateChanges[blockNumber](log)
			if err != nil {
				return nil, err
			}
			if change == nil {
				osm.logger.Sugar().Debugw("No state change found", zap.Uint64("blockNumber", blockNumber))
				return nil, nil
			}
			return change, nil
		}
	}
	return nil, nil //nolint:nilnil
}

// prepareState prepares the state for commit by adding the new state to the existing state.
func (osm *OperatorSharesModel) prepareState(blockNumber uint64) ([]*OperatorShareDeltas, error) {
	records, ok := osm.stateAccumulator[blockNumber]
	if !ok {
		msg := "delta accumulator was not initialized"
		osm.logger.Sugar().Errorw(msg, zap.Uint64("blockNumber", blockNumber))
		return nil, errors.New(msg)
	}

	return records, nil
}

func (osm *OperatorSharesModel) writeDeltaRecords(blockNumber uint64) error {
	deltas := osm.stateAccumulator[blockNumber]
	if len(deltas) == 0 {
		return nil
	}

	var block storage.Block
	res := osm.DB.Model(&storage.Block{}).Where("number = ?", blockNumber).First(&block)
	if res.Error != nil {
		osm.logger.Sugar().Errorw("Failed to fetch block", zap.Error(res.Error))
		return res.Error
	}

	for _, d := range deltas {
		d.BlockTime = block.BlockTime
		d.BlockDate = block.BlockTime.Format(time.DateOnly)
	}

	res = osm.DB.Model(&OperatorShareDeltas{}).Clauses(clause.Returning{}).Create(&deltas)
	if res.Error != nil {
		osm.logger.Sugar().Errorw("Failed to create new operator_share_deltas records", zap.Error(res.Error))
		return res.Error
	}

	return nil
}

func (osm *OperatorSharesModel) CommitFinalState(blockNumber uint64) error {
	if err := osm.writeDeltaRecords(blockNumber); err != nil {
		return err
	}

	return nil
}

func (osm *OperatorSharesModel) GenerateStateRoot(blockNumber uint64) ([]byte, error) {
	deltas, err := osm.prepareState(blockNumber)
	if err != nil {
		return nil, err
	}

	inputs := osm.sortValuesForMerkleTree(deltas)

	if len(inputs) == 0 {
		return nil, nil
	}

	fullTree, err := osm.MerkleizeEigenState(blockNumber, inputs)
	if err != nil {
		osm.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Error(err),
			zap.Uint64("blockNumber", blockNumber),
			zap.Any("inputs", inputs),
		)
		return nil, err
	}
	return fullTree.Root(), nil
}

func (osm *OperatorSharesModel) sortValuesForMerkleTree(diffs []*OperatorShareDeltas) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, diff := range diffs {
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: NewSlotID(diff.Operator, diff.Strategy, diff.Staker, diff.TransactionHash, diff.LogIndex),
			Value:  []byte(diff.Shares),
		})
	}
	slices.SortFunc(inputs, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})
	return inputs
}

func (osm *OperatorSharesModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return osm.BaseEigenState.DeleteState("operator_share_deltas", startBlockNumber, endBlockNumber, osm.DB)
}
