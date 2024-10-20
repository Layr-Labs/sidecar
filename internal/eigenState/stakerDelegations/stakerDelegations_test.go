package stakerDelegations

import (
	"database/sql"
	"testing"
	"time"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/eigenStateModel"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/Layr-Labs/go-sidecar/internal/tests/sqlite"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup() (
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	error,
) {
	cfg := tests.GetConfig()
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	db, err := sqlite.GetInMemorySqliteDatabaseConnection(l)
	if err != nil {
		panic(err)
	}
	sqliteMigrator := migrations.NewSqliteMigrator(db, l)
	if err := sqliteMigrator.MigrateAll(); err != nil {
		l.Sugar().Fatalw("Failed to migrate", "error", err)
	}

	return cfg, db, l, err
}

func teardown(model *eigenStateModel.EigenStateModel) {
	model.DB().Exec("delete from staker_delegation_changes")
	model.DB().Exec("delete from delegated_stakers")
	model.DB().Exec("delete from staker_delegation_changes")
}

func Test_DelegatedStakersState(t *testing.T) {
	cfg, grm, l, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should create a new StakerDelegationsModel", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		model, err := NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, model)
	})
	t.Run("Should register StakerDelegationsModel", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: 100,
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			Arguments:        `[{"Name":"staker","Type":"address","Value":"0xbde83df53bc7d159700e966ad5d21e8b7c619459","Indexed":true},{"Name":"operator","Type":"address","Value":"0xbde83df53bc7d159700e966ad5d21e8b7c619459","Indexed":true}]`,
			EventName:        "StakerDelegated",
			LogIndex:         400,
			OutputData:       `{}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		model, err := NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		assert.Equal(t, true, model.IsInterestingLog(&log))

		err = model.InitBlock(blockNumber)
		assert.Nil(t, err)

		res, err := model.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, res)

		typedChange := res.(*AccumulatedStateChange)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", typedChange.Staker)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", typedChange.Operator)

		teardown(model)
	})
	t.Run("Should register StakerDelegationsModel and generate the table for the block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: 100,
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			Arguments:        `[{"Name":"staker","Type":"address","Value":"0xbde83df53bc7d159700e966ad5d21e8b7c619459","Indexed":true},{"Name":"operator","Type":"address","Value":"0xbde83df53bc7d159700e966ad5d21e8b7c619459","Indexed":true}]`,
			EventName:        "StakerDelegated",
			LogIndex:         400,
			OutputData:       `{}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		model, err := NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		assert.Equal(t, true, model.IsInterestingLog(&log))

		err = model.InitBlock(blockNumber)
		assert.Nil(t, err)

		stateChange, err := model.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, stateChange)

		typedChange := stateChange.(*AccumulatedStateChange)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", typedChange.Staker)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", typedChange.Operator)

		err = model.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		states := []DelegatedStakers{}
		statesRes := model.DB().
			Model(&DelegatedStakers{}).
			Raw("select * from delegated_stakers where block_number = @blockNumber", sql.Named("blockNumber", blockNumber)).
			Scan(&states)

		if statesRes.Error != nil {
			t.Fatalf("Failed to fetch delegated_stakers: %v", statesRes.Error)
		}
		assert.Equal(t, 1, len(states))

		stateRoot, err := model.GenerateStateRoot(blockNumber)
		assert.Nil(t, err)
		assert.True(t, len(stateRoot) > 0)

		teardown(model)
	})
	t.Run("Should correctly generate state across multiple blocks", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blocks := []uint64{
			300,
			301,
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
				LogIndex:         400,
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

			err = model.InitBlock(log.BlockNumber)
			assert.Nil(t, err)

			stateChange, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, stateChange)

			err = model.CommitFinalState(log.BlockNumber)
			assert.Nil(t, err)

			states := []DelegatedStakers{}
			statesRes := model.DB().
				Model(&DelegatedStakers{}).
				Raw("select * from delegated_stakers where block_number = @blockNumber", sql.Named("blockNumber", log.BlockNumber)).
				Scan(&states)

			if statesRes.Error != nil {
				t.Fatalf("Failed to fetch delegated_stakers: %v", statesRes.Error)
			}

			if log.BlockNumber == blocks[0] {
				assert.Equal(t, 1, len(states))
				inserts, deletes, err := model.Base().(*StakerDelegationsBaseModel).prepareState(log.BlockNumber)
				assert.Nil(t, err)
				assert.Equal(t, 1, len(inserts))
				assert.Equal(t, 0, len(deletes))
			} else if log.BlockNumber == blocks[1] {
				assert.Equal(t, 0, len(states))
				inserts, deletes, err := model.Base().(*StakerDelegationsBaseModel).prepareState(log.BlockNumber)
				assert.Nil(t, err)
				assert.Equal(t, 0, len(inserts))
				assert.Equal(t, 1, len(deletes))
			}

			stateRoot, err := model.GenerateStateRoot(log.BlockNumber)
			assert.Nil(t, err)
			assert.True(t, len(stateRoot) > 0)
		}

		teardown(model)
	})
}
