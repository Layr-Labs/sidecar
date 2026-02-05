package rewards

import (
	"testing"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setupOperatorAllocationSnapshot() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	*metrics.MetricsSink,
	error,
) {
	cfg := tests.GetConfig()
	cfg.Chain = config.Chain_Holesky
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})
	sink, _ := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, nil, err
	}

	return dbname, cfg, grm, l, sink, nil
}

func teardownOperatorAllocationSnapshot(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func Test_OperatorAllocationSnapshots(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorAllocationSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should generate operator allocation snapshots", func(t *testing.T) {
		// Insert test blocks using PT1 schema (number, hash, block_time)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES 
				(100, 'hash_100', '2024-11-14 10:00:00'),
				(101, 'hash_101', '2024-11-15 14:00:00'),
				(102, 'hash_102', '2024-11-16 10:00:00')
		`)
		assert.Nil(t, res.Error)

		// Insert test operator allocations using PT1 schema
		// (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
		res = grm.Exec(`
			INSERT INTO operator_allocations (operator, avs, strategy, operator_set_id, magnitude, effective_block, block_number, transaction_hash, log_index)
			VALUES 
				('0xoperator1', '0xavs1', '0xstrategy1', 1, '1000', 100, 100, 'tx_100', 1),
				('0xoperator1', '0xavs1', '0xstrategy1', 1, '2000', 101, 101, 'tx_101', 1)
		`)
		assert.Nil(t, res.Error)

		// Generate snapshots
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2024-11-17")
		assert.Nil(t, err)

		// Verify snapshots were created
		var count int64
		res = grm.Raw(`SELECT COUNT(*) FROM operator_allocation_snapshots WHERE operator = '0xoperator1'`).Scan(&count)
		assert.Nil(t, res.Error)
		assert.True(t, count > 0, "Expected snapshots to be generated")
	})

	t.Run("Should handle empty allocations", func(t *testing.T) {
		// Clean up previous test data
		grm.Exec(`DELETE FROM operator_allocation_snapshots`)
		grm.Exec(`DELETE FROM operator_allocations WHERE operator = '0xoperator2'`)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
		assert.Nil(t, err)

		// Should not error with no allocations for this operator
		err = calculator.GenerateAndInsertOperatorAllocationSnapshots("2024-11-17")
		assert.Nil(t, err)
	})

	t.Cleanup(func() {
		teardownOperatorAllocationSnapshot(dbFileName, cfg, grm, l)
	})
}
