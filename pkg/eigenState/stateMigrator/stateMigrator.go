package stateMigrator

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator/helpers"
	_02502031222_operatorSets "github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator/stateMigrations/202502031222_operatorSets"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator/types"
	eigenStateTypes "github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/wealdtech/go-merkletree/v2"
	"github.com/wealdtech/go-merkletree/v2/keccak256"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Migrations struct {
	Migrations map[uint64][]types.IStateMigration
}

func NewMigrations() *Migrations {
	return &Migrations{
		Migrations: make(map[uint64][]types.IStateMigration),
	}
}

func (m *Migrations) RegisterMigrations(blockNumber uint64, migrations []types.IStateMigration) {
	if _, ok := m.Migrations[blockNumber]; !ok {
		m.Migrations[blockNumber] = []types.IStateMigration{}
	}
	m.Migrations[blockNumber] = append(m.Migrations[blockNumber], migrations...)
}

func (m *Migrations) GetMigrationsForBlock(blockNumber uint64) []types.IStateMigration {
	if migrations, ok := m.Migrations[blockNumber]; ok {
		return migrations
	}
	return nil
}

type StateMigrator struct {
	logger       *zap.Logger
	globalConfig *config.Config
	db           *gorm.DB
	migrations   *Migrations
}

func NewStateMigrator(grm *gorm.DB, cfg *config.Config, l *zap.Logger) (*StateMigrator, error) {
	sm := &StateMigrator{
		globalConfig: cfg,
		logger:       l,
		db:           grm,
	}

	err := sm.initialize()
	if err != nil {
		return nil, err
	}
	return sm, nil
}

// getMigrations returns a map of block numbers to migrations that should be run at that block number
// taking the chain into account
func (sm *StateMigrator) initialize() error {
	forks, err := sm.globalConfig.GetRewardsSqlForkDates()
	if err != nil {
		return err
	}
	migrations := NewMigrations()

	if mississippiFork := forks[config.RewardsFork_Mississippi]; mississippiFork.BlockNumber != 0 {
		// Mississippi hard fork. Applies to preprod and holesky to materialize EigenState models for operator sets
		migrations.RegisterMigrations(forks[config.RewardsFork_Mississippi].BlockNumber, []types.IStateMigration{
			_02502031222_operatorSets.NewStateMigration(sm.db, sm.logger, sm.globalConfig),
		})
	}
	sm.migrations = migrations
	return nil
}

// getMigrationsForBlock returns a list of migrations that should be run at the given block number
func (sm *StateMigrator) getMigrationsForBlock(blockNumber uint64) ([]types.IStateMigration, error) {
	blockMigrations := sm.migrations.GetMigrationsForBlock(blockNumber)
	if len(blockMigrations) == 0 {
		sm.logger.Sugar().Debugw("No migrations found for block", blockNumber)
		return nil, nil
	}
	return blockMigrations, nil
}

// RunMigrationsForBlock runs all migrations for the given block number and returns
// the root of a merkle tree containing all of the roots of each migration.
//
// Each migration produces a list of roots, one for each block that was processed.
func (sm *StateMigrator) RunMigrationsForBlock(blockNumber uint64) ([]byte, map[string][]interface{}, error) {
	migrations, err := sm.getMigrationsForBlock(blockNumber)
	if err != nil {
		sm.logger.Sugar().Error(err)
		return nil, nil, err
	}

	if migrations == nil {
		sm.logger.Sugar().Debugw("No migrations found for block", blockNumber)
		return nil, nil, nil
	}

	leaves := make([][]byte, 0)
	allCommittedStates := make(map[string][]interface{})
	for _, migration := range migrations {
		processedBlockRoots, committedStates, err := migration.MigrateState(blockNumber)
		if err != nil {
			sm.logger.Sugar().Errorw("Failed to run migration",
				zap.String("migration", migration.GetMigrationName()),
				zap.Error(err),
			)
			return nil, nil, err
		}
		allCommittedStates = helpers.MergeCommittedStates(allCommittedStates, committedStates)

		leaf, err := sm.convertProcessedBlockRootsToMigrationRoot(migration.GetMigrationName(), processedBlockRoots)
		if err != nil {
			sm.logger.Sugar().Errorw("Failed to convert processed block roots to migration root",
				zap.String("migration", migration.GetMigrationName()),
				zap.Error(err),
			)
			return nil, nil, err
		}
		// add a migrationRoot prefix to the leaf to make it truly unique
		leaf = append(eigenStateTypes.MerkleLeafPrefix_MigratedState, leaf...)
		leaves = append(leaves, leaf)
	}
	root, err := sm.encodeMigrationLeavesForBlock(blockNumber, leaves)
	if err != nil {
		sm.logger.Sugar().Errorw("Failed to encode migration leaves for block",
			zap.Uint64("blockNumber", blockNumber),
			zap.Error(err),
		)
		return nil, nil, err
	}
	return root, allCommittedStates, nil
}

func (sm *StateMigrator) convertProcessedBlockRootsToMigrationRoot(migrationName string, roots [][]byte) ([]byte, error) {
	// prepend migration name as a leaf of the overall tree
	roots = append([][]byte{[]byte(migrationName)}, roots...)
	tree, err := merkletree.NewTree(
		merkletree.WithData(roots),
		merkletree.WithHashType(keccak256.New()),
	)
	if err != nil {
		sm.logger.Sugar().Errorw("Failed to create merkle tree for migration",
			zap.Error(err),
		)
		return nil, err
	}
	return tree.Root(), nil
}

func (sm *StateMigrator) encodeMigrationLeavesForBlock(blockNumber uint64, leaves [][]byte) ([]byte, error) {
	tree, err := merkletree.NewTree(
		merkletree.WithData(leaves),
		merkletree.WithHashType(keccak256.New()),
	)
	if err != nil {
		sm.logger.Sugar().Errorw("Failed to create merkle tree",
			zap.Uint64("blockNumber", blockNumber),
			zap.Error(err),
		)
	}
	return tree.Root(), nil
}
