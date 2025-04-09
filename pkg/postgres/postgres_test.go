package postgres

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres/migrations"
	"os"
	"testing"
)

func Test_Postgres(t *testing.T) {
	cfg := config.NewConfig()
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	testDbName, err := tests.GenerateTestDbName()
	if err != nil {
		t.Fatalf("Failed to generate test database name: %v", err)
	}
	cfg.DatabaseConfig.DbName = testDbName

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	pgConfig := PostgresConfigFromDbConfig(&cfg.DatabaseConfig)
	pgConfig.CreateDbIfNotExists = true
	pg, err := NewPostgres(pgConfig)
	if err != nil {
		t.Fatalf("Failed to setup postgres: %v", err)
	}

	grm, err := NewGormFromPostgresConnection(pg.Db)
	if err != nil {
		t.Fatalf("Failed to create gorm instance: %v", err)
	}

	t.Run("Test Migration Up", func(t *testing.T) {
		migrator := migrations.NewMigrator(pg.Db, grm, l, cfg)
		if err = migrator.MigrateAll(); err != nil {
			t.Fatalf("Failed to migrate: %v", err)
		}
	})
}
