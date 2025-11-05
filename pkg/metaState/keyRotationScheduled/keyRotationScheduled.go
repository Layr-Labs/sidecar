package keyRotationScheduled

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

type KeyRotationScheduledModel struct {
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config

	accumulatedState map[uint64][]*types.KeyRotationScheduled
}

func NewKeyRotationScheduledModel(
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
	msm *metaStateManager.MetaStateManager,
) (*KeyRotationScheduledModel, error) {
	model := &KeyRotationScheduledModel{
		db:               db,
		logger:           logger,
		globalConfig:     globalConfig,
		accumulatedState: make(map[uint64][]*types.KeyRotationScheduled),
	}
	msm.RegisterMetaStateModel(model)
	return model, nil
}

const KeyRotationScheduledModelName = "key_rotation_scheduled"

func (krsm *KeyRotationScheduledModel) ModelName() string {
	return KeyRotationScheduledModelName
}

func (krsm *KeyRotationScheduledModel) SetupStateForBlock(blockNumber uint64) error {
	krsm.accumulatedState[blockNumber] = make([]*types.KeyRotationScheduled, 0)
	return nil
}

func (krsm *KeyRotationScheduledModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(krsm.accumulatedState, blockNumber)
	return nil
}

func (krsm *KeyRotationScheduledModel) getContractAddressesForEnvironment() map[string][]string {
	contracts := krsm.globalConfig.GetContractsMapForChain()
	return map[string][]string{
		contracts.KeyRegistrar: {
			"KeyRotationScheduled",
		},
	}
}

func (krsm *KeyRotationScheduledModel) IsInterestingLog(log *storage.TransactionLog) bool {
	contracts := krsm.getContractAddressesForEnvironment()
	return baseModel.IsInterestingLog(contracts, log)
}

type LogOutput struct {
	OperatorSet *OperatorSet `json:"operatorSet"`
	Operator    string       `json:"operator"`
	CurveType   string       `json:"curveType"`
	OldPubkey   string       `json:"oldPubkey"`
	NewPubkey   string       `json:"newPubkey"`
	ActivateAt  uint64       `json:"activateAt"`
}

type OperatorSet struct {
	Avs string `json:"avs"`
	Id  uint64 `json:"id"`
}

func (krsm *KeyRotationScheduledModel) HandleTransactionLog(log *storage.TransactionLog) (interface{}, error) {
	outputData, err := baseModel.ParseLogOutput[LogOutput](log, krsm.logger)
	if err != nil {
		return nil, err
	}

	keyRotationScheduled := &types.KeyRotationScheduled{
		Avs:             strings.ToLower(outputData.OperatorSet.Avs),
		OperatorSetId:   uint32(outputData.OperatorSet.Id),
		Operator:        strings.ToLower(outputData.Operator),
		CurveType:       strings.ToLower(outputData.CurveType),
		OldPubkey:       strings.ToLower(outputData.OldPubkey),
		NewPubkey:       strings.ToLower(outputData.NewPubkey),
		ActivateAt:      outputData.ActivateAt,
		BlockNumber:     log.BlockNumber,
		TransactionHash: log.TransactionHash,
		LogIndex:        log.LogIndex,
	}

	krsm.accumulatedState[log.BlockNumber] = append(krsm.accumulatedState[log.BlockNumber], keyRotationScheduled)
	return keyRotationScheduled, nil
}

func (krsm *KeyRotationScheduledModel) CommitFinalState(blockNumber uint64) ([]interface{}, error) {
	rowsToInsert, ok := krsm.accumulatedState[blockNumber]
	if !ok {
		return nil, fmt.Errorf("block number not initialized in accumulatedState %d", blockNumber)
	}

	if len(rowsToInsert) == 0 {
		krsm.logger.Sugar().Debugf("No key rotation scheduled records to insert for block %d", blockNumber)
		return nil, nil
	}

	res := krsm.db.Model(&types.KeyRotationScheduled{}).Clauses(clause.Returning{}).Create(&rowsToInsert)
	if res.Error != nil {
		krsm.logger.Sugar().Errorw("Failed to insert key rotation scheduled records", zap.Error(res.Error))
		return nil, res.Error
	}

	return baseModel.CastCommittedStateToInterface(rowsToInsert), nil
}

func (krsm *KeyRotationScheduledModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return baseModel.DeleteState(krsm.ModelName(), startBlockNumber, endBlockNumber, krsm.db, krsm.logger)
}
