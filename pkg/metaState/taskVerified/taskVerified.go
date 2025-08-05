package taskVerified

import (
	"fmt"
	"strings"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/metaState/baseModel"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/metaState/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type TaskVerifiedModel struct {
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	accumulatedState map[uint64][]*types.TaskVerified
}

func NewTaskVerifiedModel(
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
	msm *metaStateManager.MetaStateManager,
) (*TaskVerifiedModel, error) {
	model := &TaskVerifiedModel{
		db:               db,
		logger:           logger,
		globalConfig:     globalConfig,
		accumulatedState: make(map[uint64][]*types.TaskVerified),
	}
	msm.RegisterMetaStateModel(model)
	return model, nil
}

const TaskVerifiedModelName = "task_verified"

func (tvm *TaskVerifiedModel) ModelName() string {
	return TaskVerifiedModelName
}

func (tvm *TaskVerifiedModel) SetupStateForBlock(blockNumber uint64) error {
	tvm.accumulatedState[blockNumber] = make([]*types.TaskVerified, 0)
	return nil
}

func (tvm *TaskVerifiedModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(tvm.accumulatedState, blockNumber)
	return nil
}

func (tvm *TaskVerifiedModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := tvm.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.TaskMailbox: {
			"TaskVerified",
		},
	}
}

func (tvm *TaskVerifiedModel) IsInterestingLog(log *storage.TransactionLog) bool {
	contracts := tvm.getContractAddressesForEnvironment()
	return baseModel.IsInterestingLog(contracts, log)
}

type LogOutput struct {
	ExecutorOperatorSetId uint32 `json:"executorOperatorSetId"`
	ExecutorCert          []byte `json:"executorCert"`
	Result                []byte `json:"result"`
}

func (tvm *TaskVerifiedModel) HandleTransactionLog(log *storage.TransactionLog) (interface{}, error) {
	arguments, err := baseModel.ParseLogArguments(log, tvm.logger)
	if err != nil {
		return nil, err
	}
	outputData, err := baseModel.ParseLogOutput[LogOutput](log, tvm.logger)
	if err != nil {
		return nil, err
	}

	executorCertString := utils.ConvertBytesToString(outputData.ExecutorCert)
	resultString := utils.ConvertBytesToString(outputData.Result)

	taskVerified := &types.TaskVerified{
		Aggregator:            strings.ToLower(arguments[0].Value.(string)),
		TaskHash:              strings.ToLower(arguments[1].Value.(string)),
		Avs:                   strings.ToLower(arguments[2].Value.(string)),
		ExecutorOperatorSetId: outputData.ExecutorOperatorSetId,
		ExecutorCert:          executorCertString,
		Result:                resultString,
		BlockNumber:           log.BlockNumber,
		TransactionHash:       log.TransactionHash,
		LogIndex:              log.LogIndex,
	}

	tvm.accumulatedState[log.BlockNumber] = append(tvm.accumulatedState[log.BlockNumber], taskVerified)
	return taskVerified, nil
}

func (tvm *TaskVerifiedModel) CommitFinalState(blockNumber uint64) ([]interface{}, error) {
	rowsToInsert, ok := tvm.accumulatedState[blockNumber]
	if !ok {
		return nil, fmt.Errorf("block number not initialized in accumulatedState %d", blockNumber)
	}

	if len(rowsToInsert) == 0 {
		tvm.logger.Sugar().Debugf("No task verified records to insert for block %d", blockNumber)
		return nil, nil
	}

	res := tvm.db.Model(&types.TaskVerified{}).Clauses(clause.Returning{}).Create(&rowsToInsert)
	if res.Error != nil {
		tvm.logger.Sugar().Errorw("Failed to insert task verified records", zap.Error(res.Error))
		return nil, res.Error
	}

	return baseModel.CastCommittedStateToInterface(rowsToInsert), nil
}

func (tvm *TaskVerifiedModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return baseModel.DeleteState(tvm.ModelName(), startBlockNumber, endBlockNumber, tvm.db, tvm.logger)
}
