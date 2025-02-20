package snapshot

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/postgres/migrations"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestNewSnapshotService(t *testing.T) {
	cfg := &SnapshotConfig{}
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: false})
	svc, err := NewSnapshotService(cfg, l)
	assert.NoError(t, err, "NewSnapshotService should not return an error")
	assert.NotNil(t, svc, "SnapshotService should not be nil")
	assert.Equal(t, cfg, svc.cfg, "SnapshotConfig should match")
	assert.Equal(t, l, svc.l, "Logger should match")
}

func TestValidateCreateSnapshotConfig(t *testing.T) {
	cfg := &SnapshotConfig{
		Host:       "localhost",
		Port:       5432,
		DbName:     "testdb",
		User:       "testuser",
		Password:   "testpassword",
		SchemaName: "public",
		OutputFile: "/tmp/test_snapshot.sql",
	}
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: false})
	svc, err := NewSnapshotService(cfg, l)
	assert.NoError(t, err, "NewSnapshotService should not return an error")
	err = svc.validateCreateSnapshotConfig()
	assert.NoError(t, err, "Snapshot config should be valid")
}

func TestValidateCreateSnapshotConfigMissingOutputFile(t *testing.T) {
	cfg := &SnapshotConfig{
		Host:       "localhost",
		Port:       5432,
		DbName:     "testdb",
		User:       "testuser",
		Password:   "testpassword",
		SchemaName: "public",
		OutputFile: "",
	}
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: false})
	svc, err := NewSnapshotService(cfg, l)
	assert.NoError(t, err, "NewSnapshotService should not return an error")
	err = svc.validateCreateSnapshotConfig()
	assert.Error(t, err, "Snapshot config should be invalid if output file is missing")
}

func TestSetupSnapshotDump(t *testing.T) {
	cfg := &SnapshotConfig{
		Host:       "localhost",
		Port:       5432,
		DbName:     "testdb",
		User:       "testuser",
		Password:   "testpassword",
		SchemaName: "public",
		OutputFile: "/tmp/test_snapshot.sql",
	}
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: false})
	svc, err := NewSnapshotService(cfg, l)
	assert.NoError(t, err, "NewSnapshotService should not return an error")
	dump, err := svc.setupSnapshotDump()
	assert.NoError(t, err, "Dump setup should not fail")
	assert.NotNil(t, dump, "Dump should not be nil")
}

func TestValidateRestoreConfig(t *testing.T) {
	tempDir := t.TempDir()
	snapshotFile := filepath.Join(tempDir, "TestValidateRestoreConfig.sql")
	_, err := os.Create(snapshotFile)
	assert.NoError(t, err, "Creating snapshot file should not fail")

	cfg := &SnapshotConfig{
		Host:       "localhost",
		Port:       5432,
		DbName:     "testdb",
		User:       "testuser",
		Password:   "testpassword",
		SchemaName: "public",
		InputFile:  snapshotFile,
	}
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: false})
	svc, err := NewSnapshotService(cfg, l)
	assert.NoError(t, err, "NewSnapshotService should not return an error")
	err = svc.validateRestoreConfig()
	assert.NoError(t, err, "Restore config should be valid")
	os.Remove(snapshotFile)
}

func TestValidateRestoreConfigMissingInputFile(t *testing.T) {
	cfg := &SnapshotConfig{
		Host:       "localhost",
		Port:       5432,
		DbName:     "testdb",
		User:       "testuser",
		Password:   "testpassword",
		SchemaName: "public",
		InputFile:  "",
	}
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: false})
	svc, err := NewSnapshotService(cfg, l)
	assert.NoError(t, err, "NewSnapshotService should not return an error")
	err = svc.validateRestoreConfig()
	assert.Error(t, err, "Restore config should be invalid if input file is missing")
}

func TestSetupRestore(t *testing.T) {
	cfg := &SnapshotConfig{
		Host:       "localhost",
		Port:       5432,
		DbName:     "testdb",
		User:       "testuser",
		Password:   "testpassword",
		SchemaName: "public",
		InputFile:  "/tmp/test_snapshot.sql",
	}
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: false})
	svc, err := NewSnapshotService(cfg, l)
	assert.NoError(t, err, "NewSnapshotService should not return an error")
	restore, err := svc.setupRestore()
	assert.NoError(t, err, "Restore setup should not fail")
	assert.NotNil(t, restore, "Restore should not be nil")
}

func setup() (*config.Config, *zap.Logger, error) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Mainnet
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, err := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})
	if err != nil {
		return nil, nil, err
	}

	return cfg, l, nil
}

func TestCreateAndRestoreSnapshot(t *testing.T) {
	tempDir := t.TempDir()
	dumpFile := filepath.Join(tempDir, "TestCreateAndRestoreSnapshot.dump")

	cfg, l, setupErr := setup()
	if setupErr != nil {
		t.Fatal(setupErr)
	}

	t.Run("Create snapshot from a database with migrations", func(t *testing.T) {
		dbName, _, dbGrm, dbErr := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
		if dbErr != nil {
			t.Fatal(dbErr)
		}

		snapshotCfg := &SnapshotConfig{
			OutputFile: dumpFile,
			Host:       cfg.DatabaseConfig.Host,
			Port:       cfg.DatabaseConfig.Port,
			User:       cfg.DatabaseConfig.User,
			Password:   cfg.DatabaseConfig.Password,
			DbName:     dbName,
			SchemaName: cfg.DatabaseConfig.SchemaName,
		}

		svc, err := NewSnapshotService(snapshotCfg, l)
		assert.NoError(t, err, "NewSnapshotService should not return an error")
		err = svc.CreateSnapshot()
		assert.NoError(t, err, "Creating snapshot should not fail")

		fileInfo, err := os.Stat(dumpFile)
		assert.NoError(t, err, "Snapshot file should be created")
		assert.Greater(t, fileInfo.Size(), int64(4096), "Snapshot file size should be greater than 4KB")

		t.Cleanup(func() {
			postgres.TeardownTestDatabase(dbName, cfg, dbGrm, l)
		})
	})

	t.Run("Restore snapshot to a new database", func(t *testing.T) {
		dbName, _, dbGrm, dbErr := postgres.GetTestPostgresDatabaseWithoutMigrations(cfg.DatabaseConfig, l)
		if dbErr != nil {
			t.Fatal(dbErr)
		}

		snapshotCfg := &SnapshotConfig{
			OutputFile: "",
			InputFile:  dumpFile,
			Host:       cfg.DatabaseConfig.Host,
			Port:       cfg.DatabaseConfig.Port,
			User:       cfg.DatabaseConfig.User,
			Password:   cfg.DatabaseConfig.Password,
			DbName:     dbName,
			SchemaName: cfg.DatabaseConfig.SchemaName,
		}
		svc, err := NewSnapshotService(snapshotCfg, l)
		assert.NoError(t, err, "NewSnapshotService should not return an error")
		err = svc.RestoreSnapshot()
		assert.NoError(t, err, "Restoring snapshot should not fail")

		// Validate the restore process

		// 1) Count how many migration records already exist in db
		var countBefore int64
		dbGrm.Raw("SELECT COUNT(*) FROM migrations").Scan(&countBefore)

		// 2) Setup your migrator for db (the restored snapshot) and attempt running all migrations
		migrator := migrations.NewMigrator(nil, dbGrm, l, cfg)
		err = migrator.MigrateAll()
		assert.NoError(t, err, "Expected MigrateAll to succeed on db")

		// 3) Count again after running migrations
		var countAfter int64
		dbGrm.Raw("SELECT COUNT(*) FROM migrations").Scan(&countAfter)

		// 4) If countBefore == countAfter, no new migration records were created
		//    => meaning db was already fully up-to-date
		assert.Equal(t, countBefore, countAfter, "No migrations should have been newly applied if db matches the original")

		t.Cleanup(func() {
			postgres.TeardownTestDatabase(dbName, cfg, dbGrm, l)
		})
	})

	t.Cleanup(func() {
		os.Remove(dumpFile)
	})
}
