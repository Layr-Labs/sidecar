package snapshot

import (
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func setupCreateSnapshot() (
	string,
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	*metrics.MetricsSink,
	error,
) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Holesky
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, nil, err
	}
	cfg.DatabaseConfig.DbName = dbname

	return dbname, grm, l, cfg, sink, nil
}

func setupRestoreSnapshot() (
	string,
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	*metrics.MetricsSink,
	error,
) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Holesky
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	dbname, _, grm, err := postgres.GetTestPostgresDatabaseWithoutMigrations(cfg.DatabaseConfig, l)
	if err != nil {
		return dbname, nil, nil, nil, nil, err
	}
	cfg.DatabaseConfig.DbName = dbname

	return dbname, grm, l, cfg, sink, nil
}

func lsDir(path string) ([]os.DirEntry, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	return files, nil
}

func Test_SnapshotService(t *testing.T) {
	projectRoot := tests.GetProjectRoot()

	t.Run("Should create new snapshot service", func(t *testing.T) {
		l, err := logger.NewLogger(&logger.LoggerConfig{Debug: false})
		assert.Nil(t, err)
		sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

		ss := NewSnapshotService(l, sink)
		assert.NotNil(t, ss)
	})

	t.Run("SnapshotConfig validation", func(t *testing.T) {
		t.Run("Validate a SnapshotConfig with all params", func(t *testing.T) {
			cfg := &SnapshotConfig{
				Chain:          config.Chain_Mainnet,
				SidecarVersion: "v1.0.0",
				DBConfig: SnapshotDatabaseConfig{
					Host:     "localhost",
					Port:     5432,
					DbName:   "test_db",
					User:     "test_user",
					Password: "test_password",
				},
			}

			valid, err := cfg.IsValid()
			assert.True(t, valid)
			assert.Nil(t, err)
		})
		t.Run("Validate a SnapshotConfig with valid empty db params", func(t *testing.T) {
			cfg := &SnapshotConfig{
				Chain:          config.Chain_Mainnet,
				SidecarVersion: "v1.0.0",
				DBConfig: SnapshotDatabaseConfig{
					DbName: "test_db",
					User:   "test_user",
				},
			}

			valid, err := cfg.IsValid()
			assert.True(t, valid)
			assert.Nil(t, err)
		})
		t.Run("Validate a SnapshotConfig with empty DbName as invalid", func(t *testing.T) {
			cfg := &SnapshotConfig{
				Chain:          config.Chain_Mainnet,
				SidecarVersion: "v1.0.0",
				DBConfig: SnapshotDatabaseConfig{
					User: "test_user",
				},
			}

			valid, err := cfg.IsValid()
			assert.False(t, valid)
			assert.NotNil(t, err)
		})
	})

	t.Run("CreateSnapshot", func(t *testing.T) {
		t.Run("Validate a SnapshotConfig with empty destination path as invalid", func(t *testing.T) {
			cfg := &CreateSnapshotConfig{
				SnapshotConfig: SnapshotConfig{
					Chain:          config.Chain_Mainnet,
					SidecarVersion: "v1.0.0",
					DBConfig: SnapshotDatabaseConfig{
						DbName: "test_db",
						User:   "test_user",
					},
				},
				DestinationPath: "",
			}

			valid, err := cfg.IsValid()
			assert.False(t, valid)
			assert.NotNil(t, err)
		})
		t.Run("Should create a snapshot with hash file and metadata file", func(t *testing.T) {
			dbName, grm, l, cfg, sink, err := setupCreateSnapshot()

			if err != nil {
				t.Fatal(err)
			}

			u, err := uuid.NewRandom()
			if err != nil {
				t.Fatal(err)
			}
			destPath, err := filepath.Abs(fmt.Sprintf("%s/snapshot_test_%s", os.TempDir(), u.String()))
			if err != nil {
				t.Fatal(err)
			}
			fmt.Printf("destPath: %s\n", destPath)
			_ = os.MkdirAll(destPath, os.ModePerm)

			ss := NewSnapshotService(l, sink)
			snapshotFile, err := ss.CreateSnapshot(&CreateSnapshotConfig{
				SnapshotConfig: SnapshotConfig{
					Chain:          cfg.Chain,
					SidecarVersion: "v1.0.0",
					DBConfig:       CreateSnapshotDbConfigFromConfig(cfg.DatabaseConfig),
				},
				DestinationPath:      destPath,
				GenerateMetadataFile: true,
				Kind:                 Kind_Full,
			})
			assert.Nil(t, err)
			assert.NotNil(t, snapshotFile)

			files, err := lsDir(snapshotFile.Dir)
			assert.Nil(t, err)
			assert.Equal(t, 3, len(files))
			fmt.Printf("files: %+v\n", files)

			// shell out to sha256sum to validate the snapshot file
			cmd := exec.Command("/bin/bash", "-c", fmt.Sprintf("cd %s && sha256sum -c %s", snapshotFile.Dir, snapshotFile.HashFileName()))
			output, err := cmd.CombinedOutput()
			assert.Nil(t, err)
			assert.Contains(t, string(output), "OK")

			// open the metadata file and check the contents
			metadataContents, err := os.ReadFile(snapshotFile.MetadataFilePath())
			assert.Nil(t, err)
			assert.NotEmpty(t, metadataContents)
			fmt.Printf("metadataContents: %s\n", string(metadataContents))

			var metadata SnapshotMetadata
			err = json.Unmarshal(metadataContents, &metadata)
			assert.Nil(t, err)

			assert.Equal(t, cfg.Chain.String(), metadata.Chain)
			assert.Equal(t, snapshotFile.Version, metadata.Version)
			assert.Equal(t, snapshotFile.SchemaName, metadata.Schema)
			assert.Equal(t, snapshotFile.Kind, metadata.Kind)
			assert.Equal(t, snapshotFile.SnapshotFileName, metadata.FileName)
			assert.Equal(t, snapshotFile.CreatedTimestamp.Format(time.RFC3339), metadata.Timestamp)

			t.Cleanup(func() {
				postgres.TeardownTestDatabase(dbName, cfg, grm, l)
				_ = os.RemoveAll(snapshotFile.Dir)
			})
		})
	})

	t.Run("RestoreSnapshot", func(t *testing.T) {
		t.Run("Should restore a snapshot", func(t *testing.T) {
			var snapshotFile *SnapshotFile
			var originalMigrations []string
			t.Run("Should create a snapshot to restore from", func(t *testing.T) {
				dbName, grm, l, cfg, sink, err := setupCreateSnapshot()
				if err != nil {
					t.Fatal(err)
				}

				u, err := uuid.NewRandom()
				if err != nil {
					t.Fatal(err)
				}
				destPath, err := filepath.Abs(fmt.Sprintf("%s/snapshot_test_%s", os.TempDir(), u.String()))
				if err != nil {
					t.Fatal(err)
				}
				fmt.Printf("destPath: %s\n", destPath)
				_ = os.MkdirAll(destPath, os.ModePerm)

				ss := NewSnapshotService(l, sink)
				snapshotFile, err = ss.CreateSnapshot(&CreateSnapshotConfig{
					SnapshotConfig: SnapshotConfig{
						Chain:          cfg.Chain,
						SidecarVersion: "v1.0.0",
						DBConfig:       CreateSnapshotDbConfigFromConfig(cfg.DatabaseConfig),
					},
					DestinationPath: destPath,
					Kind:            Kind_Full,
				})
				assert.Nil(t, err)
				assert.NotNil(t, snapshotFile)

				query := `select name from migrations order by name desc`
				res := grm.Raw(query).Scan(&originalMigrations)
				if res.Error != nil {
					t.Fatal(res.Error)
				}

				t.Cleanup(func() {
					postgres.TeardownTestDatabase(dbName, cfg, grm, l)
				})
			})

			t.Run("Should restore from a snapshot", func(t *testing.T) {
				dbName, grm, l, cfg, sink, err := setupRestoreSnapshot()
				if err != nil {
					t.Fatal(err)
				}

				ss := NewSnapshotService(l, sink)
				err = ss.RestoreFromSnapshot(&RestoreSnapshotConfig{
					SnapshotConfig: SnapshotConfig{
						Chain:          cfg.Chain,
						SidecarVersion: "v1.0.0",
						DBConfig:       CreateSnapshotDbConfigFromConfig(cfg.DatabaseConfig),
					},
					Input: snapshotFile.FullPath(),
					Kind:  Kind_Full,
				})
				assert.Nil(t, err)

				var migrations []string
				query := `select name from migrations order by name desc`
				res := grm.Raw(query).Scan(&migrations)
				if res.Error != nil {
					t.Fatal(res.Error)
				}
				for i, m := range migrations {
					assert.Equal(t, originalMigrations[i], m)
				}

				t.Cleanup(func() {
					postgres.TeardownTestDatabase(dbName, cfg, grm, l)
				})
			})

			t.Cleanup(func() {
				_ = os.RemoveAll(snapshotFile.Dir)
			})
		})
	})

	t.Run("GPG verification", func(t *testing.T) {
		t.Run("Should validate a snapshot file signature", func(t *testing.T) {
			fakeSnapshotFile := fmt.Sprintf("%s/internal/tests/gpg/fake_snapshot.dump", projectRoot)

			snapshotFile := newSnapshotFile(fakeSnapshotFile)

			signers, err := snapshotFile.ValidateSignature(config.PublicGpgKey)
			assert.Nil(t, err)
			fmt.Printf("signers: %+v\n", signers)
			assert.NotNil(t, signers)
		})
		t.Run("Should fail to validate a snapshot file signature", func(t *testing.T) {
			fakeSnapshotFile := fmt.Sprintf("%s/internal/tests/gpg/fake_invalid_snapshot.dump", projectRoot)

			snapshotFile := newSnapshotFile(fakeSnapshotFile)

			signers, err := snapshotFile.ValidateSignature(config.PublicGpgKey)
			assert.NotNil(t, err)
			fmt.Printf("error: %+v\n", err)
			assert.Nil(t, signers)
		})
	})
}
