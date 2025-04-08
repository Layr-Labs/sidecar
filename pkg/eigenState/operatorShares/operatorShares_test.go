package operatorShares

import (
	"database/sql"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/logger"
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

func Test_OperatorSharesState(t *testing.T) {
	_, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should create a new OperatorSharesState", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		model, err := NewOperatorSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, model)
	})
	t.Run("Should register AvsOperatorState and generate the table for the block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		// --------------------------------------------------------------------
		// OperatorSharesIncreased
		block := storage.Block{
			Number: 19615517,
			Hash:   "some hash",
		}
		res := grm.Model(storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		transaction := storage.Transaction{
			BlockNumber:      block.Number,
			TransactionHash:  "0x07d0052ceff59634f64853b2bf716717096d74623f0294fda3bbf895d4c0c2df",
			TransactionIndex: big.NewInt(134).Uint64(),
			FromAddress:      "0x858646372cc42e1a627fce94aa7a7033e7cf075a",
		}
		res = grm.Model(storage.Transaction{}).Create(&transaction)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		log := storage.TransactionLog{
			TransactionHash:  transaction.TransactionHash,
			TransactionIndex: transaction.TransactionIndex,
			BlockNumber:      transaction.BlockNumber,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			Arguments:        `[{"Name": "operator", "Type": "address", "Value": "0xd172a86a0f250aec23ee19c759a8e73621fe3c10", "Indexed": true}, {"Name": "staker", "Type": "address", "Value": null, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": null, "Indexed": false}, {"Name": "shares", "Type": "uint256", "Value": null, "Indexed": false}]`,
			EventName:        "OperatorSharesIncreased",
			LogIndex:         big.NewInt(279).Uint64(),
			OutputData:       `{"shares": 2625783258116897034, "staker": "0x269df236ae8bd066e9de7670a7cbfd8cbafd11c2", "strategy": "0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&log)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		model, err := NewOperatorSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		change, err := model.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		err = model.CommitFinalState(block.Number, false)
		assert.Nil(t, err)

		states := []OperatorShareDeltas{}
		statesRes := model.DB.
			Raw("select * from operator_share_deltas where block_number = @blockNumber", sql.Named("blockNumber", block.Number)).
			Scan(&states)

		if statesRes.Error != nil {
			t.Fatalf("Failed to fetch operator_shares: %v", statesRes.Error)
		}
		assert.Equal(t, 1, len(states))

		assert.Equal(t, "2625783258116897034", states[0].Shares)
		assert.Equal(t, "0xd172a86a0f250aec23ee19c759a8e73621fe3c10", states[0].Operator)
		assert.Equal(t, "0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff", states[0].Strategy)
		assert.Equal(t, "0x269df236ae8bd066e9de7670a7cbfd8cbafd11c2", states[0].Staker)

		// --------------------------------------------------------------------
		// OperatorSharesDecreased

		block = storage.Block{
			Number: 20686464,
			Hash:   "some hash",
		}
		res = grm.Model(storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		transaction = storage.Transaction{
			BlockNumber:      block.Number,
			TransactionHash:  "0x7f7676903f1e3a52556dbfc212fa2af55d0aab4083a85079a5da894eb6b60c51",
			TransactionIndex: big.NewInt(60).Uint64(),
			FromAddress:      "0x858646372cc42e1a627fce94aa7a7033e7cf075a",
		}
		res = grm.Model(storage.Transaction{}).Create(&transaction)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		sharesDecreasedLog := storage.TransactionLog{
			TransactionHash:  transaction.TransactionHash,
			TransactionIndex: transaction.TransactionIndex,
			BlockNumber:      transaction.BlockNumber,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			Arguments:        `[{"Name": "operator", "Type": "address", "Value": "0xd172a86a0f250aec23ee19c759a8e73621fe3c10", "Indexed": true}, {"Name": "staker", "Type": "address", "Value": null, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": null, "Indexed": false}, {"Name": "shares", "Type": "uint256", "Value": null, "Indexed": false}]`,
			EventName:        "OperatorSharesDecreased",
			LogIndex:         big.NewInt(279).Uint64(),
			OutputData:       `{"shares": 2625783258116897034, "staker": "0x269df236ae8bd066e9de7670a7cbfd8cbafd11c2", "strategy": "0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&sharesDecreasedLog)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		err = model.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		change, err = model.HandleStateChange(&sharesDecreasedLog)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		err = model.CommitFinalState(block.Number, false)
		assert.Nil(t, err)

		states = []OperatorShareDeltas{}
		statesRes = model.DB.
			Raw("select * from operator_share_deltas where block_number = @blockNumber", sql.Named("blockNumber", block.Number)).
			Scan(&states)

		if statesRes.Error != nil {
			t.Fatalf("Failed to fetch operator_shares: %v", statesRes.Error)
		}
		assert.Equal(t, 1, len(states))

		assert.Equal(t, "-2625783258116897034", states[0].Shares)
		assert.Equal(t, "0xd172a86a0f250aec23ee19c759a8e73621fe3c10", states[0].Operator)
		assert.Equal(t, "0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff", states[0].Strategy)
		assert.Equal(t, "0x269df236ae8bd066e9de7670a7cbfd8cbafd11c2", states[0].Staker)

		query := `
			with combined_values as (
				select
					operator,
					strategy,
					transaction_hash,
					log_index,
					block_number,
					block_date,
					block_time,
					SUM(shares) OVER (PARTITION BY operator, strategy ORDER BY block_number, log_index) as shares
				from operator_share_deltas
			)
			select * from combined_values order by block_number asc, log_index asc
		`
		var rows []OperatorShareDeltas
		res = grm.Raw(query).Scan(&rows)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		assert.Equal(t, 2, len(rows))
		for i, row := range rows {
			fmt.Printf("Row %d: %+v\n", i, row)
		}
		assert.Equal(t, "0", rows[1].Shares)
	})
	t.Cleanup(func() {
		// postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
