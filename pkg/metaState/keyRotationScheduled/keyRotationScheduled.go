package keyRotationScheduled

import (
	"fmt"
	"sort"
	"strings"
	"sync"

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

	// Simple list of pending timestamps for due rotations
	pendingTimestamps []uint64
	pendingMutex      sync.RWMutex

	accumulatedEvents   map[uint64][]*types.KeyRotationScheduled
	accumulatedTriggers map[uint64][]*types.KeyRotationScheduledTrigger
}

func NewKeyRotationScheduledModel(
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
	msm *metaStateManager.MetaStateManager,
) (*KeyRotationScheduledModel, error) {
	model := &KeyRotationScheduledModel{
		db:                  db,
		logger:              logger,
		globalConfig:        globalConfig,
		pendingTimestamps:   make([]uint64, 0),
		accumulatedEvents:   make(map[uint64][]*types.KeyRotationScheduled),
		accumulatedTriggers: make(map[uint64][]*types.KeyRotationScheduledTrigger),
	}

	if err := model.loadUnprocessedRotations(); err != nil {
		logger.Sugar().Errorw("Failed to load unprocessed rotations", zap.Error(err))
		return nil, err
	}

	msm.RegisterMetaStateModel(model)
	return model, nil
}

const KeyRotationScheduledModelName = "key_rotation_scheduled"

func (krsm *KeyRotationScheduledModel) ModelName() string {
	return KeyRotationScheduledModelName
}

func (krsm *KeyRotationScheduledModel) SetupStateForBlock(blockNumber uint64) error {
	krsm.accumulatedEvents[blockNumber] = make([]*types.KeyRotationScheduled, 0)
	krsm.accumulatedTriggers[blockNumber] = make([]*types.KeyRotationScheduledTrigger, 0)
	return nil
}

func (krsm *KeyRotationScheduledModel) CleanupProcessedStateForBlock(blockNumber uint64) error {
	delete(krsm.accumulatedEvents, blockNumber)
	delete(krsm.accumulatedTriggers, blockNumber)
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
		Processed:       false, // boolean to track if daily transport was triggered for this rotation
	}

	krsm.accumulatedEvents[log.BlockNumber] = append(krsm.accumulatedEvents[log.BlockNumber], keyRotationScheduled)

	krsm.pendingMutex.Lock()
	krsm.insertTimestampSorted(keyRotationScheduled.ActivateAt)
	krsm.pendingMutex.Unlock()

	return keyRotationScheduled, nil
}

func (krsm *KeyRotationScheduledModel) CommitFinalState(blockNumber uint64) ([]interface{}, error) {
	rowsToInsert, ok := krsm.accumulatedEvents[blockNumber]
	if !ok {
		return nil, fmt.Errorf("block number not initialized in accumulatedEvents %d", blockNumber)
	}

	allResults := make([]interface{}, 0)

	if len(rowsToInsert) > 0 {
		res := krsm.db.Model(&types.KeyRotationScheduled{}).Clauses(clause.Returning{}).Create(&rowsToInsert)
		if res.Error != nil {
			krsm.logger.Sugar().Errorw("Failed to insert key rotation scheduled records", zap.Error(res.Error))
			return nil, res.Error
		}
		allResults = append(allResults, baseModel.CastCommittedStateToInterface(rowsToInsert)...)
	} else {
		krsm.logger.Sugar().Debugf("No key rotation scheduled records to insert for block %d", blockNumber)
	}

	// trigger logic: check for due timestamps
	var currentBlock storage.Block
	if err := krsm.db.Where("number = ?", blockNumber).First(&currentBlock).Error; err == nil {
		currentTime := uint64(currentBlock.BlockTime.Unix())

		krsm.pendingMutex.Lock()
		hasDueTimestamp := krsm.checkAndRemoveDueTimestamps(currentTime)
		krsm.pendingMutex.Unlock()

		if hasDueTimestamp {
			krsm.logger.Info("Found due key rotation schedules",
				zap.Uint64("blockNumber", blockNumber),
				zap.Time("blockTime", currentBlock.BlockTime))

			trigger := &types.KeyRotationScheduledTrigger{
				ActivateAt: currentBlock.BlockTime,
			}

			allResults = append(allResults, trigger)

			krsm.db.Model(&types.KeyRotationScheduled{}).
				Where("activate_at <= ? AND processed = false", currentTime).
				Update("processed", true)
		}
	}

	return allResults, nil
}

// loadUnprocessedRotations loads all unprocessed rotation timestamps from DB into memory
func (krsm *KeyRotationScheduledModel) loadUnprocessedRotations() error {
	var timestamps []uint64
	result := krsm.db.Model(&types.KeyRotationScheduled{}).
		Where("processed = false").
		Pluck("activate_at", &timestamps)
	if result.Error != nil {
		return fmt.Errorf("failed to load unprocessed timestamps: %w", result.Error)
	}

	// Sort timestamps for efficient checking
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i] < timestamps[j]
	})

	krsm.pendingMutex.Lock()
	krsm.pendingTimestamps = timestamps
	krsm.pendingMutex.Unlock()

	krsm.logger.Info("Loaded unprocessed key rotation timestamps into memory",
		zap.Int("count", len(timestamps)))

	return nil
}

func (krsm *KeyRotationScheduledModel) insertTimestampSorted(timestamp uint64) {
	idx := sort.Search(len(krsm.pendingTimestamps), func(i int) bool {
		return krsm.pendingTimestamps[i] >= timestamp
	})

	krsm.pendingTimestamps = append(krsm.pendingTimestamps, 0)
	copy(krsm.pendingTimestamps[idx+1:], krsm.pendingTimestamps[idx:])
	krsm.pendingTimestamps[idx] = timestamp
}

func (krsm *KeyRotationScheduledModel) checkAndRemoveDueTimestamps(currentTime uint64) bool {
	dueCount := 0
	for i, timestamp := range krsm.pendingTimestamps {
		if timestamp <= currentTime {
			dueCount = i + 1
		} else {
			break
		}
	}

	if dueCount > 0 {
		krsm.pendingTimestamps = krsm.pendingTimestamps[dueCount:]
		return true
	}

	return false
}

func (krsm *KeyRotationScheduledModel) DeleteState(startBlockNumber uint64, endBlockNumber uint64) error {
	return baseModel.DeleteState(krsm.ModelName(), startBlockNumber, endBlockNumber, krsm.db, krsm.logger)
}
