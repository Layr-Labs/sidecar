package stakerDelegations

import (
	"database/sql"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup() (
	string,
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	error,
) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Mainnet
	cfg.Debug = false
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func teardown(model *StakerDelegationsModel) {
	model.DB.Exec("truncate table staker_delegation_changes cascade")
	model.DB.Exec("truncate table staker_delegation_changes cascade")
}

func Test_DelegatedStakersState(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should create a new StakerDelegationsModel", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		model, err := NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, model)
	})
	t.Run("Should correctly generate state across multiple blocks", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		blocks := []uint64{
			300,
			301,
		}

		for _, blockNumber := range blocks {
			block := &storage.Block{
				Number:    blockNumber,
				Hash:      "",
				BlockTime: time.Unix(1726063248, 0),
			}
			res := grm.Model(&storage.Block{}).Create(&block)
			assert.Nil(t, res.Error)
		}

		logs := []*storage.TransactionLog{
			{
				TransactionHash:  "some hash",
				TransactionIndex: 100,
				BlockNumber:      blocks[0],
				Address:          cfg.GetContractsMapForChain().DelegationManager,
				Arguments:        `[{"Name":"staker","Type":"address","Value":"0xbde83df53bc7d159700e966ad5d21e8b7c619459","Indexed":true},{"Name":"operator","Type":"address","Value":"0xbde83df53bc7d159700e966ad5d21e8b7c619459","Indexed":true}]`,
				EventName:        "StakerDelegated",
				LogIndex:         400,
				OutputData:       `{}`,
				CreatedAt:        time.Time{},
				UpdatedAt:        time.Time{},
				DeletedAt:        time.Time{},
			},
			{
				TransactionHash:  "some hash",
				TransactionIndex: 100,
				BlockNumber:      blocks[1],
				Address:          cfg.GetContractsMapForChain().DelegationManager,
				Arguments:        `[{"Name":"staker","Type":"address","Value":"0xbde83df53bc7d159700e966ad5d21e8b7c619459","Indexed":true},{"Name":"operator","Type":"address","Value":"0xbde83df53bc7d159700e966ad5d21e8b7c619459","Indexed":true}]`,
				EventName:        "StakerUndelegated",
				LogIndex:         401,
				OutputData:       `{}`,
				CreatedAt:        time.Time{},
				UpdatedAt:        time.Time{},
				DeletedAt:        time.Time{},
			},
		}

		model, err := NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		for _, log := range logs {
			assert.True(t, model.IsInterestingLog(log))

			err = model.SetupStateForBlock(log.BlockNumber)
			assert.Nil(t, err)

			stateChange, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, stateChange)

			err = model.CommitFinalState(log.BlockNumber)
			assert.Nil(t, err)

			states := []StakerDelegationChange{}
			statesRes := model.DB.
				Raw("select * from staker_delegation_changes where block_number = @blockNumber", sql.Named("blockNumber", log.BlockNumber)).
				Scan(&states)

			if statesRes.Error != nil {
				t.Fatalf("Failed to fetch staker_delegation_changes: %v", statesRes.Error)
			}

			assert.Equal(t, 1, len(states))
			deltas, err := model.prepareState(log.BlockNumber)
			assert.Nil(t, err)
			assert.Equal(t, 1, len(deltas))

			stateRoot, err := model.GenerateStateRoot(log.BlockNumber)
			assert.Nil(t, err)
			assert.True(t, len(stateRoot) > 0)
		}

		var count int
		res := grm.Raw("select count(*) from staker_delegation_changes").Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, 2, count)

		t.Cleanup(func() {
			teardown(model)
		})
	})
	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
