package rewards

import (
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/Layr-Labs/go-sidecar/internal/tests/sqlite"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"testing"
)

func setupCombinedRewards() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	error,
) {
	cfg := tests.GetConfig()
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbName, db, err := sqlite.GetFileBasedSqliteDatabaseConnection(l)
	if err != nil {
		panic(err)
	}
	sqliteMigrator := migrations.NewSqliteMigrator(db, l)
	if err := sqliteMigrator.MigrateAll(); err != nil {
		l.Sugar().Fatalw("Failed to migrate", "error", err)
	}

	return dbName, cfg, db, l, err
}

func teardownCombinedRewards(grm *gorm.DB) {
	queries := []string{
		`delete from reward_submissions`,
		`delete from blocks`,
	}
	for _, query := range queries {
		if res := grm.Exec(query); res.Error != nil {
			fmt.Printf("Failed to run query: %v\n", res.Error)
		}
	}
}

func hydrateRewardSubmissionsTable(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetCombinedRewardsSqlFile(projectRoot)

	if err != nil {
		return err
	}

	res := grm.Exec(contents)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

func Test_CombinedRewards(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, err := setupCombinedRewards()

	testContext := getRewardsTestContext()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should hydrate blocks and reward_submissions tables", func(t *testing.T) {
		totalBlockCount, err := hydrateAllBlocksTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query := "select count(*) from blocks"
		var count int
		res := grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, totalBlockCount, count)

		err = hydrateRewardSubmissionsTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query = "select count(*) from reward_submissions"
		res = grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		switch testContext {
		case "testnet":
			assert.Equal(t, 192, count)
		case "testnet-reduced":
			assert.Equal(t, 24, count)
		case "mainnet-reduced":
			assert.Equal(t, 16, count)
		default:
			t.Fatal("Unknown test context")
		}
	})
	t.Run("Should generate the proper combinedRewards", func(t *testing.T) {
		rewards, _ := NewRewardsCalculator(l, grm, cfg)

		startDate := "1970-01-01"
		snapshotDate, err := getSnapshotDate()

		combinedRewards, err := rewards.GenerateCombinedRewards(startDate, snapshotDate)
		assert.Nil(t, err)
		assert.NotNil(t, combinedRewards)

		t.Logf("Generated %d combinedRewards", len(combinedRewards))

		switch testContext {
		case "testnet":
			assert.Equal(t, 192, len(combinedRewards))
		case "testnet-reduced":
			assert.Equal(t, 24, len(combinedRewards))
		case "mainnet-reduced":
			assert.Equal(t, 16, len(combinedRewards))
		default:
			t.Fatal("Unknown test context")
		}

	})
	t.Cleanup(func() {
		tests.DeleteTestSqliteDB(dbFileName)
		teardownCombinedRewards(grm)
	})
}
