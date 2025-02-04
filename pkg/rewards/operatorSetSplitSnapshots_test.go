package rewards

import (
	"fmt"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorSetSplits"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setupOperatorSetSplitWindows() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
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
		return "", nil, nil, nil, fmt.Errorf("Unknown test context")
	}

	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, cfg, grm, l, nil
}

func teardownOperatorSetSplitWindows(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateOperatorSetSplits(grm *gorm.DB, l *zap.Logger) error {
	activatedAt := time.Date(2024, 12, 5, 12, 0, 0, 0, time.UTC)
	split := operatorSetSplits.OperatorSetSplit{
		Operator:                "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101",
		Avs:                     "0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1",
		OperatorSetId:           1,
		ActivatedAt:             &activatedAt,
		OldOperatorSetSplitBips: 1000,
		NewOperatorSetSplitBips: 500,
		BlockNumber:             1477020,
		TransactionHash:         "0xccc83cdfa365bacff5e4099b9931bccaec1c0b0cf37cd324c92c27b5cb5387d1",
		LogIndex:                545,
	}

	result := grm.Create(&split)
	if result.Error != nil {
		l.Sugar().Errorw("Failed to create operator set split", "error", result.Error)
		return result.Error
	}
	return nil
}

func Test_OperatorSetSplitSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	// projectRoot := getProjectRootPath()
	dbFileName, cfg, grm, l, err := setupOperatorSetSplitWindows()
	if err != nil {
		t.Fatal(err)
	}
	// testContext := getRewardsTestContext()

	snapshotDate := "2024-12-09"

	t.Run("Should hydrate dependency tables", func(t *testing.T) {
		t.Log("Hydrating blocks")

		_, err := hydrateRewardsV2Blocks(grm, l)
		assert.Nil(t, err)

		t.Log("Hydrating restaked strategies")
		err = hydrateOperatorSetSplits(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query := `select count(*) from operator_set_splits`
		var count int
		res := grm.Raw(query).Scan(&count)

		assert.Nil(t, res.Error)

		assert.Equal(t, 1, count)
	})

	t.Run("Should calculate correct operatorSetSplit windows", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, l)

		t.Log("Generating snapshots")
		err := rewards.GenerateAndInsertOperatorSetSplitSnapshots(snapshotDate)
		assert.Nil(t, err)

		windows, err := rewards.ListOperatorSetSplitSnapshots()
		assert.Nil(t, err)

		t.Logf("Found %d windows", len(windows))

		assert.Equal(t, 3, len(windows))
	})
	t.Cleanup(func() {
		teardownOperatorSetSplitWindows(dbFileName, cfg, grm, l)
	})
}
