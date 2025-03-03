package avsOperators

import (
	"database/sql"
	"fmt"
	"os"
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
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func getInsertedDeltaRecords(model *AvsOperatorsModel) ([]*AvsOperatorStateChange, error) {
	results := []*AvsOperatorStateChange{}

	res := model.DB.Model(&AvsOperatorStateChange{}).Order("block_number asc").Find(&results)
	return results, res.Error
}

func Test_AvsOperatorState(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should create a new AvsOperatorState", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		avsOperatorState, err := NewAvsOperatorsModel(esm, grm, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, avsOperatorState)
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

			err = avsOperatorState.SetupStateForBlock(log.BlockNumber)
			assert.Nil(t, err)

			stateChange, err := avsOperatorState.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, stateChange)

			err = avsOperatorState.CommitFinalState(log.BlockNumber, false)
			assert.Nil(t, err)

			states := []AvsOperatorStateChange{}
			statesRes := avsOperatorState.DB.
				Raw("select * from avs_operator_state_changes where block_number = @blockNumber", sql.Named("blockNumber", log.BlockNumber)).
				Scan(&states)

			if statesRes.Error != nil {
				t.Fatalf("Failed to fetch avs_operator_state_changes: %v", statesRes.Error)
			}

			assert.Equal(t, 1, len(states))
			deltas, err := avsOperatorState.prepareState(log.BlockNumber)
			assert.Nil(t, err)
			assert.Equal(t, 1, len(deltas))

			stateRoot, err := avsOperatorState.GenerateStateRoot(log.BlockNumber)
			assert.Nil(t, err)
			assert.True(t, len(stateRoot) > 0)
		}
		type expectedValue struct {
			operator string
			avs      string
		}

		expectedValues := []expectedValue{
			{
				operator: "0xdf25bdcdcdd9a3dd8c9069306c4dba8d90dd8e8e",
				avs:      "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0",
			}, {
				operator: "0xdf25bdcdcdd9a3dd8c9069306c4dba8d90dd8e8e",
				avs:      "0x870679e138bcdf293b7ff14dd44b70fc97e12fc0",
			},
		}

		inserted, err := getInsertedDeltaRecords(avsOperatorState)
		assert.Nil(t, err)
		for i, log := range logs {
			fmt.Printf("{%d} log: %v\n", i, log)
			assert.Equal(t, expectedValues[i].operator, inserted[i].Operator)
			assert.Equal(t, expectedValues[i].avs, inserted[i].Avs)
		}

		assert.Equal(t, len(logs), len(inserted))
	})
	t.Run("Should build the state, but throw an error when inserting duplicate records", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
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

			err = avsOperatorState.SetupStateForBlock(log.BlockNumber)
			assert.Nil(t, err)

			stateChange, err := avsOperatorState.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, stateChange)

			err = avsOperatorState.CommitFinalState(log.BlockNumber, false)
			assert.NotNil(t, err)
		}
	})
	t.Run("Should build the state, but ignore the conflict for duplicate records", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
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

			err = avsOperatorState.SetupStateForBlock(log.BlockNumber)
			assert.Nil(t, err)

			stateChange, err := avsOperatorState.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, stateChange)

			err = avsOperatorState.CommitFinalState(log.BlockNumber, true)
			assert.Nil(t, err)

			var states []*AvsOperatorStateChange
			statesRes := avsOperatorState.DB.
				Raw("select * from avs_operator_state_changes where block_number = @blockNumber", sql.Named("blockNumber", log.BlockNumber)).
				Scan(&states)

			if statesRes.Error != nil {
				t.Fatalf("Failed to fetch avs_operator_state_changes: %v", statesRes.Error)
			}

			assert.Equal(t, 1, len(states))
		}
	})
	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
