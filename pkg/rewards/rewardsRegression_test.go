package rewards

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"log"
	"os"
	"testing"
)

func setupRegression(dataSourceName string) (
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	*metrics.MetricsSink,
	error,
) {
	projectRoot := tests.GetProjectRoot()
	datasources, err := tests.GetTestDataSources(projectRoot)
	if err != nil {
		log.Fatalf("Failed to get datasources: %v", err)
	}
	dsTestnet := datasources.GetDataSourceByName(dataSourceName)

	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Holesky
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()
	cfg.DatabaseConfig.DbName = dsTestnet.DbName

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)
	pg, err := postgres.NewPostgres(pgConfig)
	if err != nil {
		l.Fatal("Failed to setup postgres connection", zap.Error(err))
	}

	grm, err := postgres.NewGormFromPostgresConnection(pg.Db)
	if err != nil {
		l.Fatal("Failed to create gorm instance", zap.Error(err))
	}

	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	return grm, l, cfg, sink, nil
}

func runRegressionTest(t *testing.T, dbName string) {
	grm, l, cfg, sink, err := setupRegression(dbName)

	t.Logf("Using database with name: %s", cfg.DatabaseConfig.DbName)

	if err != nil {
		t.Fatalf("Failed to setup test: %v", err)
	}

	rewardsCalculator, err := NewRewardsCalculator(cfg, grm, nil, nil, sink, l)
	if err != nil {
		t.Fatalf("Failed to create rewards calculator: %v", err)
	}

	var distributionRoots []types.SubmittedDistributionRoot
	res := grm.Find(&types.SubmittedDistributionRoot{}).Order("block_number desc").Scan(&distributionRoots)
	if res.Error != nil {
		t.Fatalf("Failed to get distribution roots: %v", res.Error)
	}

	for i := 0; i < len(distributionRoots) && i < 15; i++ {
		distRoot := distributionRoots[i]
		fmt.Printf("Distribution root: %+v\n", distRoot)
		cutoffDate := distRoot.GetSnapshotDate()

		fmt.Printf("Using cutoff date: %v\n", cutoffDate)

		accountTree, tokenTree, distro, err := rewardsCalculator.MerkelizeRewardsForSnapshot(cutoffDate)
		if err != nil {
			t.Fatalf("Failed to merkel rewards: %v", err)
		}
		assert.NotNil(t, distro)
		assert.NotNil(t, tokenTree)

		generatedRoot := utils.ConvertBytesToString(accountTree.Root())
		fmt.Printf("Generated root: %v\n", generatedRoot)
		fmt.Printf("Existing root: %v\n", distRoot.Root)
		assert.Equal(t, distRoot.Root, generatedRoot)
	}
}

func Test_RewardsRegression(t *testing.T) {
	if !tests.LargeTestsEnabled() {
		t.Skipf("Skipping large test")
		return
	}

	// t.Run("Verify root generation against existing roots for mainnet", func(t *testing.T) {
	// 	runRegressionTest(t, "mainnetFull")
	// })

	// TODO(seanmcgary): since the mainnet database doesnt yet include changes from testnet-slashing,
	// we can only run this test on the testnet database
	t.Run("Verify root generation against existing roots for testnet", func(t *testing.T) {
		runRegressionTest(t, "testnetFull")
	})
}
