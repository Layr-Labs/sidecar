package rewards

import (
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"testing"
)

func setupRewards() (
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	error,
) {
	cfg := tests.GetConfig()
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	db, err := tests.GetSqliteDatabaseConnection(l)
	if err != nil {
		panic(err)
	}
	sqliteMigrator := migrations.NewSqliteMigrator(db, l)
	if err := sqliteMigrator.MigrateAll(); err != nil {
		l.Sugar().Fatalw("Failed to migrate", "error", err)
	}

	return cfg, db, l, err
}

func teardownRewards() {

}

func Test_Rewards(t *testing.T) {
	cfg, grm, l, err := setupOperatorAvsRegistrationSnapshot()

	if err != nil {
		t.Fatal(err)
	}

	// snapshotDate := "2024-09-01"

	t.Run("Should initialize the rewards calculator with an in-memory db", func(t *testing.T) {
		rc, err := NewRewardsCalculator(l, nil, grm, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, rc)

		query := `select name from main.sqlite_master where type = 'table' order by name asc`
		type row struct{ Name string }
		var tables []row
		res := rc.calculationDB.Raw(query).Scan(&tables)
		assert.Nil(t, res.Error)

		expectedTables := []string{
			"operator_avs_registration_snapshots",
			"operator_avs_strategy_snapshots",
			"operator_share_snapshots",
			"staker_delegation_snapshots",
			"staker_share_snapshots",
		}

		for i, table := range tables {
			assert.Equal(t, expectedTables[i], table.Name)
		}
	})
}
