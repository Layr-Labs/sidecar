package migrations

import (
	"database/sql"
	"fmt"
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
	_202501061613_reindexTestnetForStaterootChange "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501061613_reindexTestnetForStaterootChange"
	_202501071401_defaultOperatorSplitSnapshots "github.com/Layr-Labs/sidecar/pkg/postgres/migrations/202501071401_defaultOperatorSplitSnapshots"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Migration interface {
	Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error
	GetName() string
}

type Migrator struct {
	Db           *sql.DB
	GDb          *gorm.DB
	Logger       *zap.Logger
	globalConfig *config.Config
}

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
		&_202411221331_operatorAVSSplitSnapshots.Migration{},
		&_202411221331_operatorPISplitSnapshots.Migration{},
		&_202412091100_fixOperatorPiSplitsFields.Migration{},
		&_202501061029_addDescription.Migration{},
		&_202501061613_reindexTestnetForStaterootChange.Migration{},
		&_202501061422_defaultOperatorSplits.Migration{},
		&_202501071401_defaultOperatorSplitSnapshots.Migration{},
	}

	for _, migration := range migrations {
		err := m.Migrate(migration)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

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
		m.Logger.Sugar().Infof("Migration %s already run", name)
		return nil
	}
	m.Logger.Sugar().Infof("Migration %s applied", name)
	return nil
}

type Migrations struct {
	Name      string    `gorm:"primaryKey"`
	CreatedAt time.Time `gorm:"default:current_timestamp;type:timestamp with time zone"`
	UpdatedAt time.Time `gorm:"default:null;type:timestamp with time zone"`
}
