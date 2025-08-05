package generationReservationCreated

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

type GenerationReservationCreatedModel struct {
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	accumulatedState map[uint64][]*types.GenerationReservationCreated
}

func NewGenerationReservationCreatedModel(
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
	msm *metaStateManager.MetaStateManager,
) (*GenerationReservationCreatedModel, error) {
	model := &GenerationReservationCreatedModel{
		db:               db,
		logger:           logger,
		globalConfig:     globalConfig,
		accumulatedState: make(map[uint64][]*types.GenerationReservationCreated),
	}
	msm.RegisterMetaStateModel(model)
	return model, nil
}

const GenerationReservationCreatedModelName = "generation_reservation_created"

func (grcm *GenerationReservationCreatedModel) ModelName() string {
	return GenerationReservationCreatedModelName
}

func (grcm *GenerationReservationCreatedModel) SetupStateForBlock(blockNumber uint64) error {
	grcm.accumulatedState[blockNumber] = make([]*types.GenerationReservationCreated, 0)
	return nil
}

func (grcm *GenerationReservationCreatedModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(grcm.accumulatedState, blockNumber)
	return nil
}

func (grcm *GenerationReservationCreatedModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := grcm.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.CrossChainRegistry: {
			"GenerationReservationCreated",
		},
	}
}

func (grcm *GenerationReservationCreatedModel) IsInterestingLog(log *storage.TransactionLog) bool {
	contracts := grcm.getContractAddressesForEnvironment()
	return baseModel.IsInterestingLog(contracts, log)
}

type LogOutput struct {
	OperatorSet *OperatorSet `json:"operatorSet"`
}

type OperatorSet struct {
	Avs string `json:"avs"`
	Id  uint64 `json:"id"`
}

func (grcm *GenerationReservationCreatedModel) HandleTransactionLog(log *storage.TransactionLog) (interface{}, error) {
	outputData, err := baseModel.ParseLogOutput[LogOutput](log, grcm.logger)
	if err != nil {
		return nil, err
	}

	created := &types.GenerationReservationCreated{
		Avs:             strings.ToLower(outputData.OperatorSet.Avs),
		OperatorSetId:   uint64(outputData.OperatorSet.Id),
		BlockNumber:     log.BlockNumber,
		TransactionHash: log.TransactionHash,
		LogIndex:        log.LogIndex,
	}

	grcm.accumulatedState[log.BlockNumber] = append(grcm.accumulatedState[log.BlockNumber], created)
	return created, nil
}

func (grcm *GenerationReservationCreatedModel) CommitFinalState(blockNumber uint64) ([]interface{}, error) {
	rowsToInsert, ok := grcm.accumulatedState[blockNumber]
	if !ok {
		return nil, fmt.Errorf("block number not initialized in accumulatedState %d", blockNumber)
	}

	if len(rowsToInsert) == 0 {
		grcm.logger.Sugar().Debugf("No generation reservation created records to insert for block %d", blockNumber)
		return nil, nil
	}

	res := grcm.db.Model(&types.GenerationReservationCreated{}).Clauses(clause.Returning{}).Create(&rowsToInsert)
	if res.Error != nil {
		grcm.logger.Sugar().Errorw("Failed to insert generation reservation created records", zap.Error(res.Error))
		return nil, res.Error
	}

	return baseModel.CastCommittedStateToInterface(rowsToInsert), nil
}

func (grcm *GenerationReservationCreatedModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return baseModel.DeleteState(grcm.ModelName(), startBlockNumber, endBlockNumber, grcm.db, grcm.logger)
}
