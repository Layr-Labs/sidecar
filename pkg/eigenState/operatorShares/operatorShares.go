package operatorShares

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/go-sidecar/pkg/storage"
	"github.com/Layr-Labs/go-sidecar/pkg/types/numbers"
	"math/big"
	"slices"
	"sort"
	"strings"
	"time"

	pkgUtils "github.com/Layr-Labs/go-sidecar/pkg/utils"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/base"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/types"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
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
	Strategy        string
	Shares          string
	TransactionHash string
	LogIndex        uint64
	BlockNumber     uint64
	BlockTime       time.Time
	BlockDate       string
}

func NewSlotID(operator string, strategy string) types.SlotID {
	return types.SlotID(fmt.Sprintf("%s_%s", operator, strategy))
}

// Implements IEigenStateModel.
type OperatorSharesModel struct {
	base.BaseEigenState
	StateTransitions types.StateTransitions[AccumulatedStateChange]
	DB               *gorm.DB
	logger           *zap.Logger
	globalConfig     *config.Config

	// Accumulates state changes for SlotIds, grouped by block number
	stateAccumulator map[uint64]map[types.SlotID]*AccumulatedStateChange

	deltaAccumulator map[uint64][]*OperatorShareDeltas
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
		stateAccumulator: make(map[uint64]map[types.SlotID]*AccumulatedStateChange),
		deltaAccumulator: make(map[uint64][]*OperatorShareDeltas),
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

func (osm *OperatorSharesModel) GetStateTransitions() (types.StateTransitions[AccumulatedStateChange], []uint64) {
	stateChanges := make(types.StateTransitions[AccumulatedStateChange])

	stateChanges[0] = func(log *storage.TransactionLog) (*AccumulatedStateChange, error) {
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
			return nil, xerrors.Errorf("No state accumulator found for block %d", log.BlockNumber)
		}
		operator := strings.ToLower(arguments[0].Value.(string))

		sharesStr := outputData.Shares.String()
		shares, success := numbers.NewBig257().SetString(sharesStr, 10)
		if !success {
			osm.logger.Sugar().Errorw("Failed to convert shares to big.Int",
				zap.String("shares", sharesStr),
				zap.String("transactionHash", log.TransactionHash),
				zap.Uint64("transactionIndex", log.TransactionIndex),
				zap.Uint64("blockNumber", log.BlockNumber),
			)
			return nil, xerrors.Errorf("Failed to convert shares to big.Int: %s", sharesStr)
		}

		// All shares are emitted as ABS(shares), so we need to negate the shares if the event is a decrease
		if log.EventName == "OperatorSharesDecreased" {
			shares = shares.Mul(shares, big.NewInt(-1))
		}

		slotID := NewSlotID(operator, outputData.Strategy)
		record, ok := osm.stateAccumulator[log.BlockNumber][slotID]
		if !ok {
			record = &AccumulatedStateChange{
				Operator:    operator,
				Strategy:    outputData.Strategy,
				Shares:      shares,
				BlockNumber: log.BlockNumber,
			}
			osm.stateAccumulator[log.BlockNumber][slotID] = record
		} else {
			record.Shares = record.Shares.Add(record.Shares, shares)
		}

		osm.deltaAccumulator[log.BlockNumber] = append(osm.deltaAccumulator[log.BlockNumber], &OperatorShareDeltas{
			Operator:        operator,
			Strategy:        outputData.Strategy,
			Shares:          shares.String(),
			TransactionHash: log.TransactionHash,
			LogIndex:        log.LogIndex,
			BlockNumber:     log.BlockNumber,
		})

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
	osm.stateAccumulator[blockNumber] = make(map[types.SlotID]*AccumulatedStateChange)
	osm.deltaAccumulator[blockNumber] = make([]*OperatorShareDeltas, 0)
	return nil
}

func (osm *OperatorSharesModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(osm.stateAccumulator, blockNumber)
	delete(osm.deltaAccumulator, blockNumber)
	return nil
}

func (osm *OperatorSharesModel) HandleStateChange(log *storage.TransactionLog) (interface{}, error) {
	stateChanges, sortedBlockNumbers := osm.GetStateTransitions()

	for _, blockNumber := range sortedBlockNumbers {
		if log.BlockNumber >= blockNumber {
			osm.logger.Sugar().Debugw("Handling state change", zap.Uint64("blockNumber", blockNumber))

			change, err := stateChanges[blockNumber](log)
			if err != nil {
				return nil, err
			}
			if change == nil {
				return nil, xerrors.Errorf("No state change found for block %d", blockNumber)
			}
			return change, nil
		}
	}
	return nil, nil //nolint:nilnil
}

// prepareState prepares the state for commit by adding the new state to the existing state.
func (osm *OperatorSharesModel) prepareState(blockNumber uint64) ([]*OperatorSharesDiff, error) {
	preparedState := make([]*OperatorSharesDiff, 0)

	accumulatedState, ok := osm.stateAccumulator[blockNumber]
	if !ok {
		err := xerrors.Errorf("No accumulated state found for block %d", blockNumber)
		osm.logger.Sugar().Errorw(err.Error(), zap.Error(err), zap.Uint64("blockNumber", blockNumber))
		return nil, err
	}

	slotIds := make([]types.SlotID, 0)
	for slotID := range accumulatedState {
		slotIds = append(slotIds, slotID)
	}

	// Find only the records from the previous block, that are modified in this block
	query := `
		with ranked_rows as (
			select
				operator,
				strategy,
				shares,
				block_number,
				ROW_NUMBER() OVER (PARTITION BY operator, strategy ORDER BY block_number desc) as rn
			from operator_shares
			where
				concat(operator, '_', strategy) in @slotIds
		)
		select
			lb.operator,
			lb.strategy,
			lb.shares,
			lb.block_number
		from ranked_rows as lb
		where rn = 1
	`
	existingRecords := make([]*OperatorShares, 0)
	res := osm.DB.Model(&OperatorShares{}).
		Raw(query,
			sql.Named("slotIds", slotIds),
		).
		Scan(&existingRecords)

	if res.Error != nil {
		osm.logger.Sugar().Errorw("Failed to fetch operator_shares", zap.Error(res.Error))
		return nil, res.Error
	}

	// Map the existing records to a map for easier lookup
	mappedRecords := make(map[types.SlotID]*OperatorShares)
	for _, record := range existingRecords {
		slotID := NewSlotID(record.Operator, record.Strategy)
		mappedRecords[slotID] = record
	}

	// Loop over our new state changes.
	// If the record exists in the previous block, add the shares to the existing shares
	for slotID, newState := range accumulatedState {
		prepared := &OperatorSharesDiff{
			Operator:    newState.Operator,
			Strategy:    newState.Strategy,
			Shares:      newState.Shares,
			BlockNumber: blockNumber,
			IsNew:       false,
		}

		if existingRecord, ok := mappedRecords[slotID]; ok {
			existingShares, success := numbers.NewBig257().SetString(existingRecord.Shares, 10)
			if !success {
				osm.logger.Sugar().Errorw("Failed to convert existing shares to big.Int",
					zap.String("shares", existingRecord.Shares),
					zap.String("operator", existingRecord.Operator),
					zap.String("strategy", existingRecord.Strategy),
					zap.Uint64("blockNumber", blockNumber),
				)
				continue
			}
			prepared.Shares = existingShares.Add(existingShares, newState.Shares)
		} else {
			// SlotID was not found in the previous block, so this is a new record
			prepared.IsNew = true
		}

		preparedState = append(preparedState, prepared)
	}
	return preparedState, nil
}

func (osm *OperatorSharesModel) writeDeltaRecordsToDeltaTable(blockNumber uint64) error {
	deltas := osm.deltaAccumulator[blockNumber]
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
	records, err := osm.prepareState(blockNumber)
	if err != nil {
		return err
	}

	recordToInsert := pkgUtils.Map(records, func(r *OperatorSharesDiff, i uint64) *OperatorShares {
		return &OperatorShares{
			Operator:    r.Operator,
			Strategy:    r.Strategy,
			Shares:      r.Shares.String(),
			BlockNumber: blockNumber,
		}
	})

	if len(recordToInsert) > 0 {
		res := osm.DB.Model(&OperatorShares{}).Clauses(clause.Returning{}).Create(&recordToInsert)
		if res.Error != nil {
			osm.logger.Sugar().Errorw("Failed to create new operator_shares records", zap.Error(res.Error))
			return res.Error
		}
	}

	if err := osm.writeDeltaRecordsToDeltaTable(blockNumber); err != nil {
		return err
	}

	return nil
}

func (osm *OperatorSharesModel) GenerateStateRoot(blockNumber uint64) (types.StateRoot, error) {
	diffs, err := osm.prepareState(blockNumber)
	if err != nil {
		return "", err
	}

	inputs := osm.sortValuesForMerkleTree(diffs)

	fullTree, err := osm.MerkleizeState(blockNumber, inputs)
	if err != nil {
		return "", err
	}
	return types.StateRoot(pkgUtils.ConvertBytesToString(fullTree.Root())), nil
}

func (osm *OperatorSharesModel) sortValuesForMerkleTree(diffs []*OperatorSharesDiff) []*base.MerkleTreeInput {
	inputs := make([]*base.MerkleTreeInput, 0)
	for _, diff := range diffs {
		inputs = append(inputs, &base.MerkleTreeInput{
			SlotID: NewSlotID(diff.Operator, diff.Strategy),
			Value:  diff.Shares.Bytes(),
		})
	}
	slices.SortFunc(inputs, func(i, j *base.MerkleTreeInput) int {
		return strings.Compare(string(i.SlotID), string(j.SlotID))
	})
	return inputs
}

func (osm *OperatorSharesModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return osm.BaseEigenState.DeleteState("operator_shares", startBlockNumber, endBlockNumber, osm.DB)
}
