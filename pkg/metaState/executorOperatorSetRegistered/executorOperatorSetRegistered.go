package executorOperatorSetRegistered

import (
	"fmt"
	"strings"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/metaState/baseModel"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/metaState/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type ExecutorOperatorSetRegisteredModel struct {
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	accumulatedState map[uint64][]*types.ExecutorOperatorSetRegistered
}

func NewExecutorOperatorSetRegisteredModel(
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
	msm *metaStateManager.MetaStateManager,
) (*ExecutorOperatorSetRegisteredModel, error) {
	model := &ExecutorOperatorSetRegisteredModel{
		db:               db,
		logger:           logger,
		globalConfig:     globalConfig,
		accumulatedState: make(map[uint64][]*types.ExecutorOperatorSetRegistered),
	}
	msm.RegisterMetaStateModel(model)
	return model, nil
}

const ExecutorOperatorSetRegisteredModelName = "executor_operator_set_registered"

func (eosrm *ExecutorOperatorSetRegisteredModel) ModelName() string {
	return ExecutorOperatorSetRegisteredModelName
}

func (eosrm *ExecutorOperatorSetRegisteredModel) SetupStateForBlock(blockNumber uint64) error {
	eosrm.accumulatedState[blockNumber] = make([]*types.ExecutorOperatorSetRegistered, 0)
	return nil
}

func (eosrm *ExecutorOperatorSetRegisteredModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(eosrm.accumulatedState, blockNumber)
	return nil
}

func (eosrm *ExecutorOperatorSetRegisteredModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := eosrm.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.TaskMailbox: {
			"ExecutorOperatorSetRegistered",
		},
	}
}

func (eosrm *ExecutorOperatorSetRegisteredModel) IsInterestingLog(log *storage.TransactionLog) bool {
	contracts := eosrm.getContractAddressesForEnvironment()
	return baseModel.IsInterestingLog(contracts, log)
}

type LogOutput struct {
	IsRegistered bool `json:"isRegistered"`
}

func (eosrm *ExecutorOperatorSetRegisteredModel) HandleTransactionLog(log *storage.TransactionLog) (interface{}, error) {
	arguments, err := baseModel.ParseLogArguments(log, eosrm.logger)
	if err != nil {
		return nil, err
	}
	outputData, err := baseModel.ParseLogOutput[LogOutput](log, eosrm.logger)
	if err != nil {
		return nil, err
	}

	registered := &types.ExecutorOperatorSetRegistered{
		Caller:                strings.ToLower(arguments[0].Value.(string)),
		Avs:                   strings.ToLower(arguments[1].Value.(string)),
		ExecutorOperatorSetId: strings.ToLower(arguments[2].Value.(string)),
		IsRegistered:          outputData.IsRegistered,
		BlockNumber:           log.BlockNumber,
		TransactionHash:       log.TransactionHash,
		LogIndex:              log.LogIndex,
	}

	eosrm.accumulatedState[log.BlockNumber] = append(eosrm.accumulatedState[log.BlockNumber], registered)
	return registered, nil
}

func (eosrm *ExecutorOperatorSetRegisteredModel) CommitFinalState(blockNumber uint64) ([]interface{}, error) {
	rowsToInsert, ok := eosrm.accumulatedState[blockNumber]
	if !ok {
		return nil, fmt.Errorf("block number not initialized in accumulatedState %d", blockNumber)
	}

	if len(rowsToInsert) == 0 {
		eosrm.logger.Sugar().Debugf("No executor operator set registered records to insert for block %d", blockNumber)
		return nil, nil
	}

	res := eosrm.db.Model(&types.ExecutorOperatorSetRegistered{}).Clauses(clause.Returning{}).Create(&rowsToInsert)
	if res.Error != nil {
		eosrm.logger.Sugar().Errorw("Failed to insert executor operator set registered records", zap.Error(res.Error))
		return nil, res.Error
	}

	return baseModel.CastCommittedStateToInterface(rowsToInsert), nil
}

func (eosrm *ExecutorOperatorSetRegisteredModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return baseModel.DeleteState(eosrm.ModelName(), startBlockNumber, endBlockNumber, eosrm.db, eosrm.logger)
}
