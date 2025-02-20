package _02502031222_operatorSets

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState"
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
	return "02502031222_operatorSets"
}

func (sm *StateMigration) MigrateState(currentBlockNumber uint64) ([][]byte, map[string][]interface{}, error) {
	// This migration only applies to preprod and holesky. Mainnet will be released with all
	// of these features (slashing, rewards v2.1) at once rather than iteratively like we did
	// with preprod and holesky
	if sm.globalConfig.Chain == config.Chain_Mainnet {
		sm.logger.Sugar().Infof("No migration needed for mainnet")
		return nil, nil, nil
	}

	stateMan := stateManager.NewEigenStateManager(nil, sm.logger, sm.db)
	if err := eigenState.LoadEigenStateModels(stateMan, sm.db, sm.logger, sm.globalConfig); err != nil {
		sm.logger.Sugar().Errorw("Failed to load eigen state models for migration", zap.Error(err))
		return nil, nil, err
	}

	contracts := sm.globalConfig.GetContractsMapForChain()

	query := `
		select
			tl.*
		from transaction_logs as tl
		where
			tl.address = @allocationManagerAddress
			and event_name in @eventNames
			and block_number < @currentBlockNumber
		order by block_number asc, log_index asc
	`

	transactionLogs := make([]*storage.TransactionLog, 0)
	res := sm.db.Raw(query,
		sql.Named("allocationManagerAddress", contracts.AllocationManager),
		sql.Named("eventNames", []string{
			"OperatorAddedToOperatorSet",
			"OperatorRemovedFromOperatorSet",
			"StrategyAddedToOperatorSet",
			"StrategyRemovedFromOperatorSet",
		}),
		sql.Named("currentBlockNumber", currentBlockNumber),
	).Scan(&transactionLogs)

	if res.Error != nil {
		sm.logger.Sugar().Errorw("Failed to query transaction logs for migration", zap.Error(res.Error))
		return nil, nil, res.Error
	}

	blocks := make(map[uint64][]*storage.TransactionLog)
	for _, tl := range transactionLogs {
		if _, ok := blocks[tl.BlockNumber]; !ok {
			blocks[tl.BlockNumber] = make([]*storage.TransactionLog, 0)
		}
		blocks[tl.BlockNumber] = append(blocks[tl.BlockNumber], tl)
	}

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
			if err := stateMan.HandleLogStateChange(tl, false); err != nil {
				sm.logger.Sugar().Errorw("Failed to handle log state change", zap.String("transactionHash", tl.TransactionHash), zap.Uint64("logIndex", tl.LogIndex), zap.Error(err))
				return nil, nil, err
			}
		}

		committedState, err := stateMan.CommitFinalState(bn)
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
