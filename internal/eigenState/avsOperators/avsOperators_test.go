package avsOperators

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
	model.DB().Exec("delete from avs_operator_changes")
	model.DB().Exec("delete from registered_avs_operators")
	model.DB().Exec("delete from avs_operator_state_changes")
}

func getInsertedDeltaRecordsForBlock(blockNumber uint64, model *eigenStateModel.EigenStateModel) ([]*AvsOperatorStateChange, error) {
	results := []*AvsOperatorStateChange{}

	res := model.DB().Model(&AvsOperatorStateChange{}).Where("block_number = ?", blockNumber).Find(&results)
	return results, res.Error
}

func getInsertedDeltaRecords(model *eigenStateModel.EigenStateModel) ([]*AvsOperatorStateChange, error) {
	results := []*AvsOperatorStateChange{}

	res := model.DB().Model(&AvsOperatorStateChange{}).Order("block_number asc").Find(&results)
	return results, res.Error
}

func Test_AvsOperatorState(t *testing.T) {
	cfg, grm, l, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should create a new AvsOperatorState", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		avsOperatorState, err := NewAvsOperatorsModel(esm, grm, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, avsOperatorState)
	})
	t.Run("Should register AvsOperatorState", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: 100,
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().AvsDirectory,
			Arguments:        `[{"Value": "0xdf25bdcdcdd9a3dd8c9069306c4dba8d90dd8e8e" }, { "Value": "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0" }]`,
			EventName:        "OperatorAVSRegistrationStatusUpdated",
			LogIndex:         400,
			OutputData:       `{ "status": 1 }`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		avsOperatorState, err := NewAvsOperatorsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		assert.Equal(t, true, avsOperatorState.IsInterestingLog(&log))

		err = avsOperatorState.InitBlock(blockNumber)
		assert.Nil(t, err)

		res, err := avsOperatorState.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, res)

		err = avsOperatorState.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		inserted, err := getInsertedDeltaRecordsForBlock(blockNumber, avsOperatorState)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(inserted))

		assert.Equal(t, "0xdf25bdcdcdd9a3dd8c9069306c4dba8d90dd8e8e", inserted[0].Avs)
		assert.Equal(t, "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0", inserted[0].Operator)
		assert.Equal(t, true, inserted[0].Registered)
		assert.Equal(t, blockNumber, inserted[0].BlockNumber)
		assert.Equal(t, uint64(400), inserted[0].LogIndex)

		t.Cleanup(func() {
			teardown(avsOperatorState)
		})
	})
	t.Run("Should register AvsOperatorState and generate the table for the block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: 100,
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().AvsDirectory,
			Arguments:        `[{"Value": "0xdf25bdcdcdd9a3dd8c9069306c4dba8d90dd8e8e" }, { "Value": "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0" }]`,
			EventName:        "OperatorAVSRegistrationStatusUpdated",
			LogIndex:         400,
			OutputData:       `{ "status": 1 }`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		avsOperatorState, err := NewAvsOperatorsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		assert.Equal(t, true, avsOperatorState.IsInterestingLog(&log))

		err = avsOperatorState.InitBlock(blockNumber)
		assert.Nil(t, err)

		stateChange, err := avsOperatorState.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, stateChange)

		err = avsOperatorState.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		states := []RegisteredAvsOperators{}
		statesRes := avsOperatorState.DB().
			Model(&RegisteredAvsOperators{}).
			Raw("select * from registered_avs_operators where block_number = @blockNumber", sql.Named("blockNumber", blockNumber)).
			Scan(&states)

		if statesRes.Error != nil {
			t.Fatalf("Failed to fetch registered_avs_operators: %v", statesRes.Error)
		}
		assert.Equal(t, 1, len(states))

		stateRoot, err := avsOperatorState.GenerateStateRoot(blockNumber)
		assert.Nil(t, err)
		assert.True(t, len(stateRoot) > 0)

		t.Cleanup(func() {
			teardown(avsOperatorState)
		})
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
				Address:          cfg.GetContractsMapForChain().AvsDirectory,
				Arguments:        `[{"Value": "0xdf25bdcdcdd9a3dd8c9069306c4dba8d90dd8e8e" }, { "Value": "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0" }]`,
				EventName:        "OperatorAVSRegistrationStatusUpdated",
				LogIndex:         400,
				OutputData:       `{ "status": 1 }`,
				CreatedAt:        time.Time{},
				UpdatedAt:        time.Time{},
				DeletedAt:        time.Time{},
			},
			{
				TransactionHash:  "some hash",
				TransactionIndex: 100,
				BlockNumber:      blocks[1],
				Address:          cfg.GetContractsMapForChain().AvsDirectory,
				Arguments:        `[{"Value": "0xdf25bdcdcdd9a3dd8c9069306c4dba8d90dd8e8e" }, { "Value": "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0" }]`,
				EventName:        "OperatorAVSRegistrationStatusUpdated",
				LogIndex:         400,
				OutputData:       `{ "status": 0 }`,
				CreatedAt:        time.Time{},
				UpdatedAt:        time.Time{},
				DeletedAt:        time.Time{},
			},
		}

		avsOperatorState, err := NewAvsOperatorsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		for _, log := range logs {
			assert.True(t, avsOperatorState.IsInterestingLog(log))

			err = avsOperatorState.InitBlock(log.BlockNumber)
			assert.Nil(t, err)

			stateChange, err := avsOperatorState.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, stateChange)

			err = avsOperatorState.CommitFinalState(log.BlockNumber)
			assert.Nil(t, err)

			states := []RegisteredAvsOperators{}
			statesRes := avsOperatorState.DB().
				Model(&RegisteredAvsOperators{}).
				Raw("select * from registered_avs_operators where block_number = @blockNumber", sql.Named("blockNumber", log.BlockNumber)).
				Scan(&states)

			if statesRes.Error != nil {
				t.Fatalf("Failed to fetch registered_avs_operators: %v", statesRes.Error)
			}

			if log.BlockNumber == blocks[0] {
				assert.Equal(t, 1, len(states))
				inserts, deletes, err := avsOperatorState.Base().(*AvsOperatorsBaseModel).prepareState(log.BlockNumber)
				assert.Nil(t, err)
				assert.Equal(t, 1, len(inserts))
				assert.Equal(t, 0, len(deletes))
			} else if log.BlockNumber == blocks[1] {
				assert.Equal(t, 0, len(states))
				inserts, deletes, err := avsOperatorState.Base().(*AvsOperatorsBaseModel).prepareState(log.BlockNumber)
				assert.Nil(t, err)
				assert.Equal(t, 0, len(inserts))
				assert.Equal(t, 1, len(deletes))
			}

			stateRoot, err := avsOperatorState.GenerateStateRoot(log.BlockNumber)
			assert.Nil(t, err)
			assert.True(t, len(stateRoot) > 0)
		}

		inserted, err := getInsertedDeltaRecords(avsOperatorState)
		assert.Nil(t, err)
		assert.Equal(t, len(logs), len(inserted))

		t.Cleanup(func() {
			teardown(avsOperatorState)
		})
	})
}
