package _02503171455_withdrawalEvents

import (
	"context"
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/precommitProcessors"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator/helpers"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator/types"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type StateMigration struct {
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config
}

func NewStateMigration(db *gorm.DB, l *zap.Logger, cfg *config.Config) types.IStateMigration {
	return &StateMigration{
		db:           db,
		logger:       l,
		globalConfig: cfg,
	}
}

func (sm *StateMigration) GetMigrationName() string {
	return "02503171455_withdrawalEvents"
}

func (sm *StateMigration) MigrateState(currentBlockNumber uint64) ([][]byte, map[string][]interface{}, error) {
	// This migration only applies to preprod and holesky. Mainnet will be released with all
	// of these features (slashing) at once.
	if sm.globalConfig.Chain == config.Chain_Mainnet {
		sm.logger.Sugar().Infof("No migration needed for mainnet")
		return nil, nil, nil
	}

	stateMan := stateManager.NewEigenStateManager(nil, sm.logger, sm.db)
	if err := eigenState.LoadEigenStateModels(stateMan, sm.db, sm.logger, sm.globalConfig); err != nil {
		sm.logger.Sugar().Errorw("Failed to load eigen state models for migration", zap.Error(err))
		return nil, nil, err
	}

	precommitProcessors.LoadPrecommitProcessors(stateMan, sm.db, sm.logger)

	contracts := sm.globalConfig.GetContractsMapForChain()

	// find all missing events
	query := `
		select
			tl.*
		from transaction_logs as tl
		where
		    tl.block_number < @currentBlockNumber
		    and tl.address = @delegationManagerAddress
		  	and tl.event_name in @delegationManagerEventNames
		order by block_number asc, log_index asc
	`

	transactionLogs := make([]*storage.TransactionLog, 0)
	res := sm.db.Raw(query,
		sql.Named("delegationManagerAddress", contracts.DelegationManager),
		sql.Named("delegationManagerEventNames", []string{
			"SlashingWithdrawalCompleted",
			"SlashingWithdrawalQueued",
		}),
		sql.Named("currentBlockNumber", currentBlockNumber),
	).Scan(&transactionLogs)

	if res.Error != nil {
		sm.logger.Sugar().Errorw("Failed to query transaction logs for migration", zap.Error(res.Error))
		return nil, nil, res.Error
	}

	sm.logger.Sugar().Infow("Found transaction logs for migration",
		zap.Int("count", len(transactionLogs)),
		zap.Uint64("currentBlockNumber", currentBlockNumber),
		zap.String("migrationName", sm.GetMigrationName()),
	)

	blocks := make(map[uint64][]*storage.TransactionLog)
	for _, tl := range transactionLogs {
		if _, ok := blocks[tl.BlockNumber]; !ok {
			blocks[tl.BlockNumber] = make([]*storage.TransactionLog, 0)
		}
		blocks[tl.BlockNumber] = append(blocks[tl.BlockNumber], tl)
	}

	sm.logger.Sugar().Infow("Grouped into blocks",
		zap.Int("count", len(blocks)),
		zap.Uint64("currentBlockNumber", currentBlockNumber),
		zap.String("migrationName", sm.GetMigrationName()),
	)

	stateRoots := make([][]byte, 0)
	committedStates := make(map[string][]interface{})
	for bn, logs := range blocks {
		if err := stateMan.InitProcessingForBlock(bn); err != nil {
			sm.logger.Sugar().Errorw("Failed to init processing for block", zap.Uint64("blockNumber", bn), zap.Error(err))
			return nil, nil, err
		}

		// Get the stored block since the blockHash is needed for the StateManager to generate the state root
		var storedBlock *storage.Block
		res := sm.db.Model(&storage.Block{}).Where("number = ?", bn).First(&storedBlock)
		if res.Error != nil {
			sm.logger.Sugar().Errorw("Failed to query block", zap.Uint64("blockNumber", bn), zap.Error(res.Error))
			return nil, nil, res.Error
		}

		for _, tl := range logs {
			if err := stateMan.HandleLogStateChange(context.Background(), tl, false); err != nil {
				sm.logger.Sugar().Errorw("Failed to handle log state change", zap.String("transactionHash", tl.TransactionHash), zap.Uint64("logIndex", tl.LogIndex), zap.Error(err))
				return nil, nil, err
			}
		}

		if err := stateMan.RunPrecommitProcessors(bn); err != nil {
			sm.logger.Sugar().Errorw("Failed to run precommit processors", zap.Uint64("blockNumber", bn), zap.Error(err))
			return nil, nil, err
		}

		committedState, err := stateMan.CommitFinalState(bn, true)
		if err != nil {
			sm.logger.Sugar().Errorw("Failed to commit final state", zap.Uint64("blockNumber", bn), zap.Error(err))
			return nil, nil, err
		}

		// merge committed states into the single large list
		committedStates = helpers.MergeCommittedStates(committedStates, committedState)

		root, err := stateMan.GenerateStateRoot(bn, storedBlock.Hash)
		if err != nil {
			sm.logger.Sugar().Errorw("Failed to generate state root",
				zap.Uint64("blockNumber", bn),
				zap.Error(err),
			)
			return nil, nil, err
		}

		rootBytes, err := utils.ConvertStringToBytes(string(root))
		if err != nil {
			sm.logger.Sugar().Errorw("Failed to convert root to bytes",
				zap.String("root", string(root)),
				zap.Error(err),
			)
			return nil, nil, err
		}
		stateRoots = append(stateRoots, rootBytes)

		if err := stateMan.CleanupProcessedStateForBlock(bn); err != nil {
			sm.logger.Sugar().Errorw("Failed to cleanup processed state for block", zap.Uint64("blockNumber", bn), zap.Error(err))
			return nil, nil, err
		}
	}
	return stateRoots, committedStates, nil
}
