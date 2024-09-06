package migrations

import (
	"database/sql"
	"fmt"
	_202409061249_bootstrapDb "github.com/Layr-Labs/sidecar/internal/sqlite/migrations/202409061249_bootstrapDb"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"time"
)

type ISqliteMigration interface {
	Up(grm *gorm.DB) error
	GetName() string
}

type SqliteMigrator struct {
	Db     *sql.DB
	GDb    *gorm.DB
	Logger *zap.Logger
}

func NewSqliteMigrator(gDb *gorm.DB, l *zap.Logger) *SqliteMigrator {
	return &SqliteMigrator{
		GDb:    gDb,
		Logger: l,
	}
}

func (m *SqliteMigrator) MigrateAll() error {
	err := m.CreateMigrationTablesIfNotExists()
	if err != nil {
		return err
	}

	migrations := []ISqliteMigration{
		&_202409061249_bootstrapDb.SqliteMigration{},
	}

	for _, migration := range migrations {
		err := m.Migrate(migration)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func (m *SqliteMigrator) CreateMigrationTablesIfNotExists() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS migrations (
			name TEXT PRIMARY KEY,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT NULL,
			deleted_at DATETIME DEFAULT NULL
		)`,
	}

	for _, query := range queries {
		res := m.GDb.Exec(query)
		if res.Error != nil {
			m.Logger.Sugar().Errorw("Failed to create migration table", zap.Error(res.Error))
			return res.Error
		}
	}
	return nil
}

func (m *SqliteMigrator) Migrate(migration ISqliteMigration) error {
	name := migration.GetName()

	// find migration by name
	var migrationRecord Migrations
	result := m.GDb.Find(&migrationRecord, "name = ?", name).Limit(1)

	if result.Error == nil && result.RowsAffected == 0 {
		m.Logger.Sugar().Infof("Running migration '%s'", name)
		// run migration
		err := migration.Up(m.GDb)
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
	return nil
}

type Migrations struct {
	Name      string `gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
}