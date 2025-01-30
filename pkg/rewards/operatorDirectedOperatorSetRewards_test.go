package rewards

import (
	"testing"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setupOperatorDirectedOperatorSetRewards() (
	string,
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	error,
) {
	cfg := tests.GetConfig()
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, cfg, grm, l, nil
}

func teardownOperatorDirectedOperatorSetRewards(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func hydrateOperatorDirectedOperatorSetRewardSubmissionsTable(grm *gorm.DB, l *zap.Logger) error {
	query := `
		INSERT INTO operator_directed_operator_set_reward_submissions (
			avs, 
			operator_set_id, 
			reward_hash,
			token,
			operator,
			operator_index,
			amount,
			strategy,
			strategy_index,
			multiplier,
			start_timestamp,
			end_timestamp,
			duration,
			description,
			block_number,
			transaction_hash,
			log_index
		)
		VALUES (
			'0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101',
			1,
			'0x7402669fb2c8a0cfe8108acb8a0070257c77ec6906ecb07d97c38e8a5ddc66a9',
			'0x0ddd9dc88e638aef6a8e42d0c98aaa6a48a98d24',
			'0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1',
			0,
			'30000000000000000000000',
			'0x5074dfd18e9498d9e006fb8d4f3fecdc9af90a2c',
			0,
			'1000000000000000000',
			to_timestamp(1725494400),
			to_timestamp(1725494400 + 2419200),
			2419200,
			'test reward submission',
			1477020,
			'some hash',
			12
		)
	`

	res := grm.Exec(query)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

func Test_OperatorDirectedOperatorSetRewards(t *testing.T) {
	if !rewardsTestsEnabled() {
		t.Skipf("Skipping %s", t.Name())
		return
	}

	dbFileName, cfg, grm, l, err := setupOperatorDirectedOperatorSetRewards()

	if err != nil {
		t.Fatal(err)
	}

	snapshotDate := "2024-12-09"

	t.Run("Should hydrate blocks and operator_directed_operator_set_reward_submissions tables", func(t *testing.T) {
		t.Log("Hydrating blocks")
		totalBlockCount, err := hydrateRewardsV2Blocks(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		t.Log("Hydrating operator directed operator set reward submissions")
		query := "select count(*) from blocks"
		var count int
		res := grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, totalBlockCount, count)

		err = hydrateOperatorDirectedOperatorSetRewardSubmissionsTable(grm, l)
		if err != nil {
			t.Fatal(err)
		}

		query = "select count(*) from operator_directed_operator_set_reward_submissions"
		res = grm.Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, count)
	})

	t.Run("Should generate the proper operatorDirectedOperatorSetRewards", func(t *testing.T) {
		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
		rewards, _ := NewRewardsCalculator(cfg, grm, nil, sog, l)

		err = rewards.GenerateAndInsertOperatorDirectedOperatorSetRewards(snapshotDate)
		assert.Nil(t, err)

		operatorDirectedOperatorSetRewards, err := rewards.ListOperatorDirectedOperatorSetRewards()
		assert.Nil(t, err)

		assert.NotNil(t, operatorDirectedOperatorSetRewards)

		t.Logf("Generated %d operatorDirectedOperatorSetRewards", len(operatorDirectedOperatorSetRewards))

		assert.Equal(t, 1, len(operatorDirectedOperatorSetRewards))
	})

	t.Cleanup(func() {
		teardownOperatorDirectedOperatorSetRewards(dbFileName, cfg, grm, l)
	})
}
