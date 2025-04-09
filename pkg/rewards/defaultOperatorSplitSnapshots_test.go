package rewards

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setupDefaultOperatorSplitWindows() (
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

func teardownDefaultOperatorSplitWindows(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateDefaultOperatorSplits(grm *gorm.DB, l *zap.Logger) error {
	query := `
		INSERT INTO default_operator_splits (old_default_operator_split_bips, new_default_operator_split_bips, block_number, transaction_hash, log_index)
		VALUES (1000, 500, '1477020', '0xccc83cdfa365bacff5e4099b9931bccaec1c0b0cf37cd324c92c27b5cb5387d1', 545)
	`

	res := grm.Exec(query)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

func Test_DefaultOperatorSplitSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	// projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, sink, err := setupDefaultOperatorSplitWindows()
	if err != nil {
		t.Fatal(err)
	}
	// testContext := getRewardsTestContext()

	snapshotDate := "2024-12-09"

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		t.Log("Hydrating blocks")

		_, err := hydrateRewardsV2Blocks(grm, l)
		assert.Nil(t, err)

		t.Log("Hydrating default operator splits")
		err = hydrateDefaultOperatorSplits(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query := `select count(*) from default_operator_splits`
		var count int
		res := grm.Raw(query).Scan(&count)

		assert.Nil(t, res.Error)

		assert.Equal(t, 1, count)
	})

	t.Run("Should calculate correct default operator split windows", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		t.Log("Generating snapshots")
		err := rewards.GenerateAndInsertDefaultOperatorSplitSnapshots(snapshotDate)
		assert.Nil(t, err)

		windows, err := rewards.ListDefaultOperatorSplitSnapshots()
		assert.Nil(t, err)

		t.Logf("Found %d windows", len(windows))

		assert.Equal(t, 218, len(windows))
	})
	t.Cleanup(func() {
		teardownDefaultOperatorSplitWindows(dbFileName, cfg, grm, l)
	})
}

func Test_NoDefaultOperatorSplitSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	// projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, sink, err := setupDefaultOperatorSplitWindows()
	if err != nil {
		t.Fatal(err)
	}
	// testContext := getRewardsTestContext()

	snapshotDate := "2024-12-09"

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		t.Log("Hydrating blocks")

		_, err := hydrateRewardsV2Blocks(grm, l)
		assert.Nil(t, err)

		// No hydration of default operator splits
		query := `select count(*) from default_operator_splits`
		var count int
		res := grm.Raw(query).Scan(&count)

		assert.Nil(t, res.Error)

		assert.Equal(t, 0, count)
	})

	t.Run("Should calculate correct default operator split windows", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

		t.Log("Generating snapshots")
		err := rewards.GenerateAndInsertDefaultOperatorSplitSnapshots(snapshotDate)
		assert.Nil(t, err)

		windows, err := rewards.ListDefaultOperatorSplitSnapshots()
		assert.Nil(t, err)

		t.Logf("Found %d windows", len(windows))

		assert.Equal(t, 0, len(windows))
	})
	t.Cleanup(func() {
		teardownDefaultOperatorSplitWindows(dbFileName, cfg, grm, l)
	})
}
