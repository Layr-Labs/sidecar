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

func Test_OperatorSetOperatorRegistrationSnapshots_SlashableUntil(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, sink, err := setupOperatorSetOperatorRegistrationSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer teardownOperatorSetOperatorRegistrationSnapshot(dbFileName, cfg, grm, l)

	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, _ := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)

	t.Run("Test 1: Operator deregisters - slashable_until should be deregistration + 14 days", func(t *testing.T) {
		// Clean up from previous tests
		grm.Exec("TRUNCATE TABLE operator_set_operator_registrations CASCADE")
		grm.Exec("TRUNCATE TABLE operator_set_operator_registration_snapshots CASCADE")
		grm.Exec("DELETE FROM blocks WHERE number >= 100")

		// Setup: Operator registers day 1, deregisters day 5
		// Expected: snapshots days 2-5 with slashable_until = day 19 (day 5 + 14)

		// Create blocks
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES
				(100, 'hash_100', '2024-11-01 10:00:00', '2024-11-01', 'root', NOW(), NOW()),
				(105, 'hash_105', '2024-11-05 10:00:00', '2024-11-05', 'root', NOW(), NOW())
		`)
		assert.Nil(t, res.Error)

		// Create registration event at day 1
		res = grm.Exec(`
			INSERT INTO operator_set_operator_registrations (operator, avs, operator_set_id, is_active, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES ('0xoperator1', '0xavs1', 1, true, 100, 'tx_100', 1, NOW(), NOW())
		`)
		assert.Nil(t, res.Error)

		// Create deregistration event at day 5
		res = grm.Exec(`
			INSERT INTO operator_set_operator_registrations (operator, avs, operator_set_id, is_active, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES ('0xoperator1', '0xavs1', 1, false, 105, 'tx_105', 1, NOW(), NOW())
		`)
		assert.Nil(t, res.Error)

		// Generate snapshots
		snapshotDate := "2024-11-20"
		err = rc.GenerateAndInsertOperatorSetOperatorRegistrationSnapshots(snapshotDate)
		assert.Nil(t, err)

		// Query snapshots
		var snapshots []struct {
			Snapshot       string
			SlashableUntil string
		}
		res = grm.Raw(`
			SELECT
				to_char(snapshot, 'YYYY-MM-DD') as snapshot,
				to_char(slashable_until, 'YYYY-MM-DD') as slashable_until
			FROM operator_set_operator_registration_snapshots
			WHERE operator = '0xoperator1' AND avs = '0xavs1' AND operator_set_id = 1
			ORDER BY snapshot
		`).Scan(&snapshots)
		assert.Nil(t, res.Error)

		t.Logf("Found %d snapshots", len(snapshots))
		// Should have snapshots for days 2-5 (registration rounds up, deregistration rounds down)
		assert.Equal(t, 4, len(snapshots))

		// All snapshots should have slashable_until = November 19 (day 5 + 14 days)
		for _, snapshot := range snapshots {
			assert.Equal(t, "2024-11-19", snapshot.SlashableUntil,
				fmt.Sprintf("Snapshot %s should have slashable_until = 2024-11-19", snapshot.Snapshot))
		}
	})

	t.Run("Test 2: Active operator - slashable_until should be NULL", func(t *testing.T) {
		// Clean up
		grm.Exec("TRUNCATE TABLE operator_set_operator_registrations CASCADE")
		grm.Exec("TRUNCATE TABLE operator_set_operator_registration_snapshots CASCADE")
		grm.Exec("DELETE FROM blocks WHERE number >= 200")

		// Setup: Operator registers, never deregisters
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES (200, 'hash_200', '2024-12-01 10:00:00', '2024-12-01', 'root', NOW(), NOW())
		`)
		assert.Nil(t, res.Error)

		// Create registration event
		res = grm.Exec(`
			INSERT INTO operator_set_operator_registrations (operator, avs, operator_set_id, is_active, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES ('0xoperator2', '0xavs2', 2, true, 200, 'tx_200', 1, NOW(), NOW())
		`)
		assert.Nil(t, res.Error)

		// Generate snapshots
		snapshotDate := "2024-12-10"
		err = rc.GenerateAndInsertOperatorSetOperatorRegistrationSnapshots(snapshotDate)
		assert.Nil(t, err)

		// Query snapshots
		var snapshots []struct {
			SlashableUntil *string
		}
		res = grm.Raw(`
			SELECT slashable_until
			FROM operator_set_operator_registration_snapshots
			WHERE operator = '0xoperator2' AND avs = '0xavs2' AND operator_set_id = 2
			LIMIT 1
		`).Scan(&snapshots)
		assert.Nil(t, res.Error)
		assert.Greater(t, len(snapshots), 0)

		// slashable_until should be NULL for active operators
		assert.Nil(t, snapshots[0].SlashableUntil)
	})

	t.Run("Test 3: Multiple deregistrations - each period has correct slashable_until", func(t *testing.T) {
		// Clean up
		grm.Exec("TRUNCATE TABLE operator_set_operator_registrations CASCADE")
		grm.Exec("TRUNCATE TABLE operator_set_operator_registration_snapshots CASCADE")
		grm.Exec("DELETE FROM blocks WHERE number >= 300")

		// Setup: Register day 1, deregister day 5, re-register day 20, deregister day 25
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date, state_root, created_at, updated_at)
			VALUES
				(300, 'hash_300', '2025-01-01 10:00:00', '2025-01-01', 'root', NOW(), NOW()),
				(305, 'hash_305', '2025-01-05 10:00:00', '2025-01-05', 'root', NOW(), NOW()),
				(320, 'hash_320', '2025-01-20 10:00:00', '2025-01-20', 'root', NOW(), NOW()),
				(325, 'hash_325', '2025-01-25 10:00:00', '2025-01-25', 'root', NOW(), NOW())
		`)
		assert.Nil(t, res.Error)

		// Registration events
		res = grm.Exec(`
			INSERT INTO operator_set_operator_registrations (operator, avs, operator_set_id, is_active, block_number, transaction_hash, log_index, created_at, updated_at)
			VALUES
				('0xoperator3', '0xavs3', 3, true, 300, 'tx_300', 1, NOW(), NOW()),
				('0xoperator3', '0xavs3', 3, false, 305, 'tx_305', 1, NOW(), NOW()),
				('0xoperator3', '0xavs3', 3, true, 320, 'tx_320', 1, NOW(), NOW()),
				('0xoperator3', '0xavs3', 3, false, 325, 'tx_325', 1, NOW(), NOW())
		`)
		assert.Nil(t, res.Error)

		// Generate snapshots
		snapshotDate := "2025-02-15"
		err = rc.GenerateAndInsertOperatorSetOperatorRegistrationSnapshots(snapshotDate)
		assert.Nil(t, err)

		// Query first period snapshots
		var firstPeriod []struct {
			Snapshot       string
			SlashableUntil string
		}
		res = grm.Raw(`
			SELECT
				to_char(snapshot, 'YYYY-MM-DD') as snapshot,
				to_char(slashable_until, 'YYYY-MM-DD') as slashable_until
			FROM operator_set_operator_registration_snapshots
			WHERE operator = '0xoperator3' AND avs = '0xavs3' AND operator_set_id = 3
			  AND snapshot >= '2025-01-02' AND snapshot <= '2025-01-05'
			ORDER BY snapshot
		`).Scan(&firstPeriod)
		assert.Nil(t, res.Error)
		assert.Greater(t, len(firstPeriod), 0)

		// First period should have slashable_until = January 19 (day 5 + 14)
		for _, snapshot := range firstPeriod {
			assert.Equal(t, "2025-01-19", snapshot.SlashableUntil,
				fmt.Sprintf("First period snapshot %s should have slashable_until = 2025-01-19", snapshot.Snapshot))
		}

		// Query second period snapshots
		var secondPeriod []struct {
			Snapshot       string
			SlashableUntil string
		}
		res = grm.Raw(`
			SELECT
				to_char(snapshot, 'YYYY-MM-DD') as snapshot,
				to_char(slashable_until, 'YYYY-MM-DD') as slashable_until
			FROM operator_set_operator_registration_snapshots
			WHERE operator = '0xoperator3' AND avs = '0xavs3' AND operator_set_id = 3
			  AND snapshot >= '2025-01-21' AND snapshot <= '2025-01-25'
			ORDER BY snapshot
		`).Scan(&secondPeriod)
		assert.Nil(t, res.Error)
		assert.Greater(t, len(secondPeriod), 0)

		// Second period should have slashable_until = February 8 (day 25 + 14)
		for _, snapshot := range secondPeriod {
			assert.Equal(t, "2025-02-08", snapshot.SlashableUntil,
				fmt.Sprintf("Second period snapshot %s should have slashable_until = 2025-02-08", snapshot.Snapshot))
		}
	})
}
