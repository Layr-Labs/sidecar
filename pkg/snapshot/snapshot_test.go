package snapshot

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
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
	l, _ := zap.NewDevelopment()
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
	l, _ := zap.NewDevelopment()
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
	l, _ := zap.NewDevelopment()
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
	l, _ := zap.NewDevelopment()
	svc, err := NewSnapshotService(cfg, l)
	assert.NoError(t, err, "NewSnapshotService should not return an error")
	dump, err := svc.setupSnapshotDump()
	assert.NoError(t, err, "Dump setup should not fail")
	assert.NotNil(t, dump, "Dump should not be nil")
}

func TestValidateRestoreConfig(t *testing.T) {
	tempDir := t.TempDir()
	snapshotFile := filepath.Join(tempDir, "TestValidateRestoreConfig.sql")
	snapshotHashFile := filepath.Join(tempDir, "TestValidateRestoreConfig.sql.sha256sum")
	content := []byte("test content")
	err := os.WriteFile(snapshotFile, content, 0644)
	assert.NoError(t, err, "Writing to snapshot file should not fail")

	// Correct the hash to match the expected value and include the file name
	expectedHash := "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72"
	hashFileContent := fmt.Sprintf("%s %s\n", expectedHash, filepath.Base(snapshotFile))
	err = os.WriteFile(snapshotHashFile, []byte(hashFileContent), 0644)
	assert.NoError(t, err, "Writing to hash file should not fail")

	cfg := &SnapshotConfig{
		Host:          "localhost",
		Port:          5432,
		DbName:        "testdb",
		User:          "testuser",
		Password:      "testpassword",
		SchemaName:    "public",
		InputFile:     snapshotFile,
		InputHashFile: snapshotHashFile,
	}
	l, _ := zap.NewDevelopment()
	svc, err := NewSnapshotService(cfg, l)
	assert.NoError(t, err, "NewSnapshotService should not return an error")
	err = svc.validateRestoreConfig()
	assert.NoError(t, err, "Restore config should be valid")
	os.Remove(snapshotFile)
	os.Remove(snapshotHashFile)
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
	l, _ := zap.NewDevelopment()
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
	l, _ := zap.NewDevelopment()
	svc, err := NewSnapshotService(cfg, l)
	assert.NoError(t, err, "NewSnapshotService should not return an error")
	restore, err := svc.setupRestore()
	assert.NoError(t, err, "Restore setup should not fail")
	assert.NotNil(t, restore, "Restore should not be nil")
}

func TestSaveOutputFileHash(t *testing.T) {
	tempDir := t.TempDir()
	outputFile := filepath.Join(tempDir, "TestSaveOutputFileHash.sql")
	outputHashFile := outputFile + ".sha256sum"
	_, err := os.Create(outputFile)
	assert.NoError(t, err, "Creating output file should not fail")

	// Use the pure function directly
	err = saveOutputFileHash(outputFile, outputHashFile)
	assert.NoError(t, err, "Saving output file hash should not fail")

	hash, err := os.ReadFile(outputHashFile)
	assert.NoError(t, err, "Reading output hash file should not fail")
	assert.NotEmpty(t, hash, "Output hash file should not be empty")

	// Validate the hash file content
	hashParts := strings.Fields(string(hash))
	assert.Equal(t, 2, len(hashParts), "Output hash file should contain two parts: hash and filename")
	assert.Equal(t, filepath.Base(outputFile), hashParts[1], "Output hash file should contain the correct filename")
	assert.Equal(t, sha256.Size*2, len(hashParts[0]), "Output hash file should have the correct hash length")
}

func TestCleanup(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "TestCleanup.tmp")
	_, err := os.Create(tempFile)
	assert.NoError(t, err, "Creating temp file should not fail")

	// Use the standalone Cleanup function
	logger, _ := zap.NewDevelopment()
	cleanup([]string{tempFile}, logger)

	_, err = os.Stat(tempFile)
	if !os.IsNotExist(err) {
		// Attempt to remove the file if it wasn't removed by the cleanup
		removeErr := os.Remove(tempFile)
		assert.NoError(t, removeErr, "Removing temp file manually should not fail")
	}
	assert.True(t, os.IsNotExist(err), "Temp file should be removed")
}

func TestValidateInputFileHash(t *testing.T) {
	tempDir := t.TempDir()
	inputFile := filepath.Join(tempDir, "TestValidateInputFileHash.sql")
	hashFile := filepath.Join(tempDir, "TestValidateInputFileHash.sql.sha256sum")
	content := []byte("test content")
	err := os.WriteFile(inputFile, content, 0644)
	assert.NoError(t, err, "Writing to input file should not fail")

	// Use the saveOutputFileHash function to create the hash file
	err = saveOutputFileHash(inputFile, hashFile)
	assert.NoError(t, err, "Saving input file hash should not fail")

	err = validateInputFileHash(inputFile, hashFile)
	assert.NoError(t, err, "Input file hash should be valid")
}

func TestGetFileHash(t *testing.T) {
	tempDir := t.TempDir()
	inputFile := filepath.Join(tempDir, "TestGetFileHash.sql")
	content := []byte("test content")
	err := os.WriteFile(inputFile, content, 0644)
	assert.NoError(t, err, "Writing to input file should not fail")

	hash, err := getFileHash(inputFile)
	assert.NoError(t, err, "Getting file hash should not fail")

	expectedHash := sha256.Sum256(content)
	assert.Equal(t, expectedHash[:], hash, "File hash should match expected hash")
}

func TestDownloadFile(t *testing.T) {
	// Create a test server that serves a simple text file
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "This is a test file.")
	}))
	defer testServer.Close()

	// Use the test server's URL to test the downloadFile function
	filePath, err := downloadFile(testServer.URL, "testfile")
	assert.NoError(t, err, "downloadFile should not return an error")

	// Schedule the file for removal after the test completes
	defer func() {
		if err := os.Remove(filePath); err != nil {
			t.Logf("Failed to remove downloaded file: %v", err)
		}
	}()

	// Verify the file was downloaded correctly
	content, err := os.ReadFile(filePath)
	assert.NoError(t, err, "Reading downloaded file should not fail")
	assert.Contains(t, string(content), "This is a test file.", "Downloaded file content should match expected content")
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
	dumpFileHash := filepath.Join(tempDir, "TestCreateAndRestoreSnapshot.dump.sha256sum")

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
			InputFile:     dumpFile,
			InputHashFile: dumpFileHash,
			Host:          cfg.DatabaseConfig.Host,
			Port:          cfg.DatabaseConfig.Port,
			User:          cfg.DatabaseConfig.User,
			Password:      cfg.DatabaseConfig.Password,
			DbName:        dbName,
			SchemaName:    cfg.DatabaseConfig.SchemaName,
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
		os.Remove(dumpFileHash)
	})
}
