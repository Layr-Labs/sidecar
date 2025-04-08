package rewards

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetOperatorRegistrations"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setupOperatorSetOperatorRegistrationSnapshot() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	*metrics.MetricsSink,
	error,
) {
	testContext := getRewardsTestContext()
	cfg := tests.GetConfig()
	switch testContext {
	case "testnet":
		cfg.Chain = config.Chain_Holesky
	case "testnet-reduced":
		cfg.Chain = config.Chain_Holesky
	case "mainnet-reduced":
		cfg.Chain = config.Chain_Mainnet
	default:
		return "", nil, nil, nil, nil, fmt.Errorf("Unknown test context")
	}

	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, nil, err
	}

	return dbname, cfg, grm, l, sink, nil
}

func teardownOperatorSetOperatorRegistrationSnapshot(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateOperatorSetOperatorRegistrationsTable(grm *gorm.DB, l *zap.Logger) error {
	registration := operatorSetOperatorRegistrations.OperatorSetOperatorRegistration{
		Operator:        "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101",
		Avs:             "0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1",
		OperatorSetId:   1,
		IsActive:        true,
		BlockNumber:     1477020,
		TransactionHash: "0xccc83cdfa365bacff5e4099b9931bccaec1c0b0cf37cd324c92c27b5cb5387d1",
		LogIndex:        545,
	}

	result := grm.Create(&registration)
	if result.Error != nil {
		l.Sugar().Errorw("Failed to create operator set operator registration", "error", result.Error)
		return result.Error
	}
	return nil
}

func Test_OperatorSetOperatorRegistrationSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	// projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, sink, err := setupOperatorSetOperatorRegistrationSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	// testContext := getRewardsTestContext()

	snapshotDate := "2024-12-09"

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		t.Log("Hydrating blocks")

		_, err := hydrateRewardsV2Blocks(grm, l)
		assert.Nil(t, err)

		t.Log("Hydrating operator set operator registrations")
		err = hydrateOperatorSetOperatorRegistrationsTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query := `select count(*) from operator_set_operator_registrations`
		var count int
		res := grm.Raw(query).Scan(&count)

		assert.Nil(t, res.Error)

		assert.Equal(t, 1, count)
	})

	t.Run("Should generate the proper operatorSetOperatorRegistrationSnapshots", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		err := rewards.GenerateAndInsertOperatorSetOperatorRegistrationSnapshots(snapshotDate)
		assert.Nil(t, err)

		snapshots, err := rewards.ListOperatorSetOperatorRegistrationSnapshots()
		assert.Nil(t, err)

		t.Logf("Found %d snapshots", len(snapshots))

		assert.Equal(t, 218, len(snapshots))
	})

	t.Cleanup(func() {
		teardownOperatorSetOperatorRegistrationSnapshot(dbFileName, cfg, grm, l)
	})
}
