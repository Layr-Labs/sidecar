package taskCreated

import (
	"encoding/json"
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

type TaskCreatedModel struct {
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	accumulatedState map[uint64][]*types.TaskCreated
}

func NewTaskCreatedModel(
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
	msm *metaStateManager.MetaStateManager,
) (*TaskCreatedModel, error) {
	model := &TaskCreatedModel{
		db:               db,
		logger:           logger,
		globalConfig:     globalConfig,
		accumulatedState: make(map[uint64][]*types.TaskCreated),
	}
	msm.RegisterMetaStateModel(model)
	return model, nil
}

const TaskCreatedModelName = "task_created"

func (tcm *TaskCreatedModel) ModelName() string {
	return TaskCreatedModelName
}

func (tcm *TaskCreatedModel) SetupStateForBlock(blockNumber uint64) error {
	tcm.accumulatedState[blockNumber] = make([]*types.TaskCreated, 0)
	return nil
}

func (tcm *TaskCreatedModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(tcm.accumulatedState, blockNumber)
	return nil
}

func (tcm *TaskCreatedModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := tcm.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.TaskMailbox: {
			"TaskCreated",
		},
	}
}

func (tcm *TaskCreatedModel) IsInterestingLog(log *storage.TransactionLog) bool {
	contracts := tcm.getContractAddressesForEnvironment()
	return baseModel.IsInterestingLog(contracts, log)
}

type LogOutput struct {
	ExecutorOperatorSetId uint32      `json:"executorOperatorSetId"`
	RefundCollector       string      `json:"refundCollector"`
	AvsFee                json.Number `json:"avsFee"`
	TaskDeadline          json.Number `json:"taskDeadline"`
	Payload               []byte      `json:"payload"`
}

func (tcm *TaskCreatedModel) HandleTransactionLog(log *storage.TransactionLog) (interface{}, error) {
	arguments, err := baseModel.ParseLogArguments(log, tcm.logger)
	if err != nil {
		return nil, err
	}
	outputData, err := baseModel.ParseLogOutput[LogOutput](log, tcm.logger)
	if err != nil {
		return nil, err
	}

	taskHashString := utils.ConvertBytesToString(arguments[1].Value.([]byte))
	payloadString := utils.ConvertBytesToString(outputData.Payload)

	taskCreated := &types.TaskCreated{
		Creator:               strings.ToLower(arguments[0].Value.(string)),
		TaskHash:              taskHashString,
		Avs:                   strings.ToLower(arguments[2].Value.(string)),
		ExecutorOperatorSetId: outputData.ExecutorOperatorSetId,
		RefundCollector:       strings.ToLower(outputData.RefundCollector),
		AvsFee:                outputData.AvsFee.String(),
		TaskDeadline:          outputData.TaskDeadline.String(),
		Payload:               payloadString,
		BlockNumber:           log.BlockNumber,
		TransactionHash:       log.TransactionHash,
		LogIndex:              log.LogIndex,
	}

	tcm.accumulatedState[log.BlockNumber] = append(tcm.accumulatedState[log.BlockNumber], taskCreated)
	return taskCreated, nil
}

func (tcm *TaskCreatedModel) CommitFinalState(blockNumber uint64) ([]interface{}, error) {
	rowsToInsert, ok := tcm.accumulatedState[blockNumber]
	if !ok {
		return nil, fmt.Errorf("block number not initialized in accumulatedState %d", blockNumber)
	}

	if len(rowsToInsert) == 0 {
		tcm.logger.Sugar().Debugf("No task created records to insert for block %d", blockNumber)
		return nil, nil
	}

	res := tcm.db.Model(&types.TaskCreated{}).Clauses(clause.Returning{}).Create(&rowsToInsert)
	if res.Error != nil {
		tcm.logger.Sugar().Errorw("Failed to insert task created records", zap.Error(res.Error))
		return nil, res.Error
	}

	return baseModel.CastCommittedStateToInterface(rowsToInsert), nil
}

func (tcm *TaskCreatedModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return baseModel.DeleteState(tcm.ModelName(), startBlockNumber, endBlockNumber, tcm.db, tcm.logger)
}
