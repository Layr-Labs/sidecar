package metaStateManager

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/metaState/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type MetaStateManager struct {
	db              *gorm.DB
	logger          *zap.Logger
	globalConfig    *config.Config
	metaStateModels []types.IMetaStateModel
}

func NewMetaStateManager(db *gorm.DB, l *zap.Logger, gc *config.Config) *MetaStateManager {
	return &MetaStateManager{
		db:              db,
		logger:          l,
		globalConfig:    gc,
		metaStateModels: make([]types.IMetaStateModel, 0),
	}
}

func (msm *MetaStateManager) RegisterMetaStateModel(model types.IMetaStateModel) {
	msm.metaStateModels = append(msm.metaStateModels, model)
}

func (msm *MetaStateManager) InitProcessingForBlock(blockNumber uint64) error {
	for _, model := range msm.metaStateModels {
		if err := model.SetupStateForBlock(blockNumber); err != nil {
			msm.logger.Sugar().Errorw("Failed to setup state for block",
				"blockNumber", blockNumber,
				"model", model,
				"error", err,
			)
			return err
		}
	}
	return nil
}

func (msm *MetaStateManager) CleanupProcessedStateForBlock(blockNumber uint64) error {
	for _, model := range msm.metaStateModels {
		if err := model.CleanupProcessedStateForBlock(blockNumber); err != nil {
			msm.logger.Sugar().Errorw("Failed to cleanup state for block",
				"blockNumber", blockNumber,
				"model", model,
				"error", err,
			)
			return err
		}
	}
	return nil
}

func (msm *MetaStateManager) HandleTransactionLog(log *storage.TransactionLog) error {
	for _, model := range msm.metaStateModels {
		if model.IsInterestingLog(log) {
			if _, err := model.HandleTransactionLog(log); err != nil {
				msm.logger.Sugar().Errorw("Failed to handle log",
					"log", log,
					"model", model,
					"error", err,
				)
				return err
			}
		}
	}
	return nil
}

func (msm *MetaStateManager) CommitFinalState(blockNumber uint64) (map[string][]interface{}, error) {
	committedState := make(map[string][]interface{})
	for _, model := range msm.metaStateModels {
		state, err := model.CommitFinalState(blockNumber)
		if err != nil {
			msm.logger.Sugar().Errorw("Failed to commit final state",
				"blockNumber", blockNumber,
				"model", model,
				"error", err,
			)
			return nil, err
		}
		committedState[model.ModelName()] = state
	}
	return committedState, nil
}
