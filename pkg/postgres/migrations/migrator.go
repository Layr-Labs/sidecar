package migrations

import (
	"database/sql"
	"fmt"
	"go.uber.org/zap"
	"gorm.io/gorm"

	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	_202409061249_bootstrapDb "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202409061249_bootstrapDb"
	_202409061250_eigenlayerStateTables "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202409061250_eigenlayerStateTables"
	_202409061720_operatorShareChanges "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202409061720_operatorShareChanges"
	_202409062151_stakerDelegations "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202409062151_stakerDelegations"
	_202409080918_staterootTable "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202409080918_staterootTable"
	_202409082234_stakerShare "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202409082234_stakerShare"
	_202409101144_submittedDistributionRoot "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202409101144_submittedDistributionRoot"
	_202409101540_rewardSubmissions "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202409101540_rewardSubmissions"
	_202409161057_avsOperatorDeltas "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202409161057_avsOperatorDeltas"
	_202409181340_stakerDelegationDelta "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202409181340_stakerDelegationDelta"
	_202410241239_combinedRewards "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202410241239_combinedRewards"
	_202410241313_operatorAvsRegistrationSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202410241313_operatorAvsRegistrationSnapshots"
	_202410241417_operatorAvsStrategySnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202410241417_operatorAvsStrategySnapshots"
	_202410241431_operatorShareSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202410241431_operatorShareSnapshots"
	_202410241450_stakerDelegationSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202410241450_stakerDelegationSnapshots"
	_202410241456_stakerShareSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202410241456_stakerShareSnapshots"
	_202410241539_goldTables "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202410241539_goldTables"
	_202410301449_generatedRewardsSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202410301449_generatedRewardsSnapshots"
	_202411041043_blockNumberFkConstraint "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411041043_blockNumberFkConstraint"
	_202411041332_stakerShareDeltaBlockFk "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411041332_stakerShareDeltaBlockFk"
	_202411042033_cleanupDuplicates "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411042033_cleanupDuplicates"
	_202411051308_submittedDistributionRootIndex "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411051308_submittedDistributionRootIndex"
	_202411061451_transactionLogsIndex "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411061451_transactionLogsIndex"
	_202411061501_stakerSharesReimagined "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411061501_stakerSharesReimagined"
	_202411071011_updateOperatorSharesDelta "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411071011_updateOperatorSharesDelta"
	_202411081039_operatorRestakedStrategiesConstraint "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411081039_operatorRestakedStrategiesConstraint"
	_202411120947_disabledDistributionRoots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411120947_disabledDistributionRoots"
	_202411130953_addHashColumns "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411130953_addHashColumns"
	_202411131200_eigenStateModelConstraints "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411131200_eigenStateModelConstraints"
	_202411151931_operatorDirectedRewardSubmissions "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411151931_operatorDirectedRewardSubmissions"
	_202411191550_operatorAVSSplits "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411191550_operatorAVSSplits"
	_202411191708_operatorPISplits "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411191708_operatorPISplits"
	_202411191947_cleanupUnusedTables "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411191947_cleanupUnusedTables"
	_202411221331_operatorAVSSplitSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411221331_operatorAVSSplitSnapshots"
	_202411221331_operatorPISplitSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202411221331_operatorPISplitSnapshots"
	_202412021311_stakerOperatorTables "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202412021311_stakerOperatorTables"
	_202412061553_addBlockNumberIndexes "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202412061553_addBlockNumberIndexes"
	_202412061626_operatorRestakedStrategiesConstraint "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202412061626_operatorRestakedStrategiesConstraint"
	_202412091100_fixOperatorPiSplitsFields "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202412091100_fixOperatorPiSplitsFields"
	_202501061029_addDescription "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501061029_addDescription"
	_202501061422_defaultOperatorSplits "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501061422_defaultOperatorSplits"
	_202501071401_defaultOperatorSplitSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501071401_defaultOperatorSplitSnapshots"
	_202501151039_rewardsClaimed "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501151039_rewardsClaimed"
	_202501241111_addIndexesForRpcFunctions "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501241111_addIndexesForRpcFunctions"
	_202501241322_operatorDirectedOperatorSetRewardSubmissions "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501241322_operatorDirectedOperatorSetRewardSubmissions"
	_202501241533_operatorSetSplits "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501241533_operatorSetSplits"
	_202501271727_operatorSetOperatorRegistrations "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501271727_operatorSetOperatorRegistrations"
	_202501281806_operatorSetStrategyRegistrations "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501281806_operatorSetStrategyRegistrations"
	_202501301458_operatorSetSplitSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501301458_operatorSetSplitSnapshots"
	_202501301502_operatorSetOperatorRegistrationSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501301502_operatorSetOperatorRegistrationSnapshots"
	_202501301505_operatorSetStrategyRegistrationSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501301505_operatorSetStrategyRegistrationSnapshots"
	_202501301945_operatorDirectedOperatorSetRewards "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501301945_operatorDirectedOperatorSetRewards"
	_202502051830_addOperatorSetIdToStakerOperator "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202502051830_addOperatorSetIdToStakerOperator"
	_202502100846_goldTableRewardHashIndex "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202502100846_goldTableRewardHashIndex"
	_202502180836_snapshotUniqueConstraints "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202502180836_snapshotUniqueConstraints"
	_202502211539_hydrateClaimedRewards "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202502211539_hydrateClaimedRewards"
	_202502252204_slashingModels "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202502252204_slashingModels"
	_202503030846_cleanupConstraintNames "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202503030846_cleanupConstraintNames"
	_202503042014_stakerOperatorIndex "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202503042014_stakerOperatorIndex"
	_202503051449_addContractTypeColumn "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202503051449_addContractTypeColumn"
	_202503061009_pectraPrune "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202503061009_pectraPrune"
	_202503061223_renameConstraint "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202503061223_renameConstraint"
	_202503130907_pectraPrunePartTwo "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202503130907_pectraPrunePartTwo"
	_202503171414_slashingWithdrawals "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202503171414_slashingWithdrawals"
	_202503191610_coreContractMigrations "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202503191610_coreContractMigrations"
	_202503311108_goldRewardHashIndex "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202503311108_goldRewardHashIndex"
	_202504240743_fixQueuedSlashingWithdrawalsPk "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202504240743_fixQueuedSlashingWithdrawalsPk"
	_202505092007_startupJobs "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202505092007_startupJobs"
	_202506172149_snapshotUniqueConstraintsPartTwo "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202506172149_snapshotUniqueConstraintsPartTwo"
)

// Migration interface defines the contract for database migrations.
// Each migration must implement the Up method to apply changes and GetName to identify itself.
type Migration interface {
	// Up applies the migration to the database
	Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error
	// GetName returns the unique name of the migration
	GetName() string
}

// Migrator handles the execution and tracking of database migrations.
type Migrator struct {
	// Db is the raw SQL database connection
	Db *sql.DB
	// GDb is the GORM database connection
	GDb *gorm.DB
	// Logger for logging migration operations
	Logger *zap.Logger
	// globalConfig contains application configuration
	globalConfig *config.Config
}

// NewMigrator creates a new Migrator instance and initializes the migrations table.
//
// Parameters:
//   - db: SQL database connection
//   - gDb: GORM database connection
//   - l: Logger for logging migration operations
//   - cfg: Global application configuration
//
// Returns:
//   - *Migrator: A new Migrator instance
func NewMigrator(db *sql.DB, gDb *gorm.DB, l *zap.Logger, cfg *config.Config) *Migrator {
	err := initializeMigrationTable(gDb)
	if err != nil {
		l.Sugar().Fatalw("Failed to auto-migrate migrations table", zap.Error(err))
	}
	return &Migrator{
		Db:           db,
		GDb:          gDb,
		Logger:       l,
		globalConfig: cfg,
	}
}

// initializeMigrationTable creates the migrations table if it doesn't exist.
// This table tracks which migrations have been applied to the database.
//
// Parameters:
//   - db: GORM database connection
//
// Returns:
//   - error: Any error encountered during table creation
func initializeMigrationTable(db *gorm.DB) error {
	query := `
		create table if not exists migrations (
    		name text primary key,
    		created_at timestamp with time zone default current_timestamp,
            updated_at timestamp with time zone default null
		)`
	result := db.Exec(query)
	return result.Error
}

// MigrateAll runs all available migrations in sequence.
// If any migration fails, the function will panic.
//
// Returns:
//   - error: Any error encountered during migration
func (m *Migrator) MigrateAll() error {
	migrations := []Migration{
		&_202409061249_bootstrapDb.Migration{},
		&_202409061250_eigenlayerStateTables.Migration{},
		&_202409061720_operatorShareChanges.Migration{},
		&_202409062151_stakerDelegations.Migration{},
		&_202409080918_staterootTable.Migration{},
		&_202409082234_stakerShare.Migration{},
		&_202409101144_submittedDistributionRoot.Migration{},
		&_202409101540_rewardSubmissions.Migration{},
		&_202409161057_avsOperatorDeltas.Migration{},
		&_202409181340_stakerDelegationDelta.Migration{},
		&_202410241239_combinedRewards.Migration{},
		&_202410241313_operatorAvsRegistrationSnapshots.Migration{},
		&_202410241417_operatorAvsStrategySnapshots.Migration{},
		&_202410241431_operatorShareSnapshots.Migration{},
		&_202410241450_stakerDelegationSnapshots.Migration{},
		&_202410241456_stakerShareSnapshots.Migration{},
		&_202410241539_goldTables.Migration{},
		&_202410301449_generatedRewardsSnapshots.Migration{},
		&_202411041043_blockNumberFkConstraint.Migration{},
		&_202411041332_stakerShareDeltaBlockFk.Migration{},
		&_202411042033_cleanupDuplicates.Migration{},
		&_202411051308_submittedDistributionRootIndex.Migration{},
		&_202411061451_transactionLogsIndex.Migration{},
		&_202411061501_stakerSharesReimagined.Migration{},
		&_202411071011_updateOperatorSharesDelta.Migration{},
		&_202411081039_operatorRestakedStrategiesConstraint.Migration{},
		&_202411120947_disabledDistributionRoots.Migration{},
		&_202411130953_addHashColumns.Migration{},
		&_202411131200_eigenStateModelConstraints.Migration{},
		&_202411151931_operatorDirectedRewardSubmissions.Migration{},
		&_202411191550_operatorAVSSplits.Migration{},
		&_202411191708_operatorPISplits.Migration{},
		&_202411191947_cleanupUnusedTables.Migration{},
		&_202412021311_stakerOperatorTables.Migration{},
		&_202412061553_addBlockNumberIndexes.Migration{},
		&_202412061626_operatorRestakedStrategiesConstraint.Migration{},
		&_202501151039_rewardsClaimed.Migration{},
		&_202411221331_operatorAVSSplitSnapshots.Migration{},
		&_202411221331_operatorPISplitSnapshots.Migration{},
		&_202412091100_fixOperatorPiSplitsFields.Migration{},
		&_202501061029_addDescription.Migration{},
		&_202501061422_defaultOperatorSplits.Migration{},
		&_202501071401_defaultOperatorSplitSnapshots.Migration{},
		&_202501241111_addIndexesForRpcFunctions.Migration{},
		&_202502100846_goldTableRewardHashIndex.Migration{},
		&_202502211539_hydrateClaimedRewards.Migration{},
		&_202503042014_stakerOperatorIndex.Migration{},
		&_202502180836_snapshotUniqueConstraints.Migration{},
		&_202503051449_addContractTypeColumn.Migration{},
		&_202503311108_goldRewardHashIndex.Migration{},
		&_202501241322_operatorDirectedOperatorSetRewardSubmissions.Migration{},
		&_202501241533_operatorSetSplits.Migration{},
		&_202501271727_operatorSetOperatorRegistrations.Migration{},
		&_202501281806_operatorSetStrategyRegistrations.Migration{},
		&_202501301458_operatorSetSplitSnapshots.Migration{},
		&_202501301502_operatorSetOperatorRegistrationSnapshots.Migration{},
		&_202501301505_operatorSetStrategyRegistrationSnapshots.Migration{},
		&_202501301945_operatorDirectedOperatorSetRewards.Migration{},
		&_202502051830_addOperatorSetIdToStakerOperator.Migration{},
		&_202503030846_cleanupConstraintNames.Migration{},
		&_202502252204_slashingModels.Migration{},
		&_202503061009_pectraPrune.Migration{},
		&_202503061223_renameConstraint.Migration{},
		&_202503130907_pectraPrunePartTwo.Migration{},
		&_202503171414_slashingWithdrawals.Migration{},
		&_202504240743_fixQueuedSlashingWithdrawalsPk.Migration{},
		&_202505092007_startupJobs.Migration{},
		&_202506172149_snapshotUniqueConstraintsPartTwo.Migration{},
		&_202503191610_coreContractMigrations.Migration{},
	}

	for _, migration := range migrations {
		err := m.Migrate(migration)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

// Migrate runs a single migration if it hasn't been applied yet.
//
// Parameters:
//   - migration: The migration to apply
//
// Returns:
//   - error: Any error encountered during migration
func (m *Migrator) Migrate(migration Migration) error {
	name := migration.GetName()

	// find migration by name
	var migrationRecord Migrations
	result := m.GDb.Find(&migrationRecord, "name = ?", name).Limit(1)

	if result.Error == nil && result.RowsAffected == 0 {
		m.Logger.Sugar().Debugf("Running migration '%s'", name)
		// run migration
		err := migration.Up(m.Db, m.GDb, m.globalConfig)
		if err != nil {
			m.Logger.Sugar().Errorw(fmt.Sprintf("Failed to run migration '%s'", name), zap.Error(err))
			return err
		}

		// record migration
		migrationRecord = Migrations{
			Name: name,
		}
		result = m.GDb.Create(&migrationRecord)
		if result.Error != nil {
			m.Logger.Sugar().Errorw(fmt.Sprintf("Failed to record migration '%s'", name), zap.Error(result.Error))
			return result.Error
		}
	} else if result.Error != nil {
		m.Logger.Sugar().Errorw(fmt.Sprintf("Failed to find migration '%s'", name), zap.Error(result.Error))
		return result.Error
	} else if result.RowsAffected > 0 {
		m.Logger.Sugar().Debugf("Migration %s already run", name)
		return nil
	}
	m.Logger.Sugar().Debugf("Migration %s applied", name)
	return nil
}

// Migrations represents a record of a migration that has been applied to the database.
type Migrations struct {
	// Name is the unique identifier of the migration
	Name string `gorm:"primaryKey"`
	// CreatedAt is the timestamp when the migration was applied
	CreatedAt time.Time `gorm:"default:current_timestamp;type:timestamp with time zone"`
	// UpdatedAt is the timestamp when the migration was last updated
	UpdatedAt time.Time `gorm:"default:null;type:timestamp with time zone"`
}
