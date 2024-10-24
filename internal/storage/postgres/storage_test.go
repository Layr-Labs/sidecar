package postgres

import (
	"github.com/Layr-Labs/go-sidecar/internal/postgres"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/parser"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
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
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: true})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func teardown(dbname string, cfg *config.Config, db *gorm.DB, l *zap.Logger) {
	rawDb, _ := db.DB()
	_ = rawDb.Close()

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	if err := postgres.DeleteTestDatabase(pgConfig, dbname); err != nil {
		l.Sugar().Errorw("Failed to delete test database", "error", err)
	}
}

func Test_PostgresqlBlockstore(t *testing.T) {
	dbname, db, l, cfg, err := setup()
	if err != nil {
		t.Fatalf("Failed to setup: %v", err)
	}
	blockStore := NewPostgresBlockStore(db, l, cfg)

	insertedBlocks := make([]*storage.Block, 0)
	insertedTransactions := make([]*storage.Transaction, 0)

	t.Run("Blocks", func(t *testing.T) {

		t.Run("InsertBlockAtHeight", func(t *testing.T) {
			block := &storage.Block{
				Number:    100,
				Hash:      "some hash",
				BlockTime: time.Now(),
			}

			insertedBlock, err := blockStore.InsertBlockAtHeight(block.Number, block.Hash, uint64(block.BlockTime.Unix()))
			if err != nil {
				t.Errorf("Failed to insert block: %v", err)
			}
			assert.NotNil(t, insertedBlock)
			assert.Equal(t, block.Number, insertedBlock.Number)
			assert.Equal(t, block.Hash, insertedBlock.Hash)

			insertedBlocks = append(insertedBlocks, insertedBlock)
		})
		t.Run("Fail to insert a duplicate block", func(t *testing.T) {
			block := &storage.Block{
				Number:    100,
				Hash:      "some hash",
				BlockTime: time.Now(),
			}

			_, err := blockStore.InsertBlockAtHeight(block.Number, block.Hash, uint64(block.BlockTime.Unix()))
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "duplicate key value violates unique constraint")
		})
	})
	t.Run("Transactions", func(t *testing.T) {
		block := insertedBlocks[0]

		t.Run("InsertBlockTransaction", func(t *testing.T) {
			tx := storage.Transaction{
				BlockNumber:      block.Number,
				TransactionHash:  "txHash",
				TransactionIndex: 0,
				FromAddress:      "from",
				ToAddress:        "to",
				ContractAddress:  "contractAddress",
				BytecodeHash:     "bytecodeHash",
			}
			insertedTx, err := blockStore.InsertBlockTransaction(
				tx.BlockNumber,
				tx.TransactionHash,
				tx.TransactionIndex,
				tx.FromAddress,
				tx.ToAddress,
				tx.ContractAddress,
				tx.BytecodeHash,
			)
			assert.Nil(t, err)
			assert.NotNil(t, insertedTx)
			assert.Equal(t, tx.BlockNumber, insertedTx.BlockNumber)
			assert.Equal(t, tx.TransactionHash, insertedTx.TransactionHash)
			assert.Equal(t, tx.TransactionIndex, insertedTx.TransactionIndex)
			assert.Equal(t, tx.FromAddress, insertedTx.FromAddress)
			assert.Equal(t, tx.ToAddress, insertedTx.ToAddress)
			assert.Equal(t, strings.ToLower(tx.ContractAddress), insertedTx.ContractAddress)
			assert.Equal(t, tx.BytecodeHash, insertedTx.BytecodeHash)

			insertedTransactions = append(insertedTransactions, insertedTx)
		})
		t.Run("Fail to insert a duplicate transaction", func(t *testing.T) {
			tx := storage.Transaction{
				BlockNumber:      block.Number,
				TransactionHash:  "txHash",
				TransactionIndex: 0,
				FromAddress:      "from",
				ToAddress:        "to",
				ContractAddress:  "contractAddress",
				BytecodeHash:     "bytecodeHash",
			}
			_, err := blockStore.InsertBlockTransaction(
				tx.BlockNumber,
				tx.TransactionHash,
				tx.TransactionIndex,
				tx.FromAddress,
				tx.ToAddress,
				tx.ContractAddress,
				tx.BytecodeHash,
			)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "duplicate key value violates unique constraint")
		})
	})
	t.Run("TransactionLogs", func(t *testing.T) {
		t.Run("InsertTransactionLog", func(t *testing.T) {
			decodedLog := &parser.DecodedLog{
				LogIndex: 0,
				Address:  "log-address",
				Arguments: []parser.Argument{
					{
						Name:    "arg1",
						Type:    "string",
						Value:   "some-value",
						Indexed: true,
					},
				},
				EventName: "SomeEvent",
				OutputData: map[string]interface{}{
					"output": "data",
				},
			}

			// jsonArguments, _ := json.Marshal(decodedLog.Arguments)
			// jsonOutputData, _ := json.Marshal(decodedLog.OutputData)

			txLog := &storage.TransactionLog{
				TransactionHash:  insertedTransactions[0].TransactionHash,
				TransactionIndex: insertedTransactions[0].TransactionIndex,
				BlockNumber:      insertedTransactions[0].BlockNumber,
			}

			insertedTxLog, err := blockStore.InsertTransactionLog(
				txLog.TransactionHash,
				txLog.TransactionIndex,
				txLog.BlockNumber,
				decodedLog,
				decodedLog.OutputData,
			)
			assert.Nil(t, err)

			assert.Equal(t, txLog.TransactionHash, insertedTxLog.TransactionHash)
			assert.Equal(t, txLog.TransactionIndex, insertedTxLog.TransactionIndex)
			assert.Equal(t, txLog.BlockNumber, insertedTxLog.BlockNumber)
			assert.Equal(t, decodedLog.Address, insertedTxLog.Address)
			assert.Equal(t, decodedLog.EventName, insertedTxLog.EventName)
			assert.Equal(t, decodedLog.LogIndex, insertedTxLog.LogIndex)
			// assert.Equal(t, string(jsonArguments), insertedTxLog.Arguments)
			// assert.Equal(t, string(jsonOutputData), insertedTxLog.OutputData)
		})
		t.Run("Fail to insert a duplicate transaction log", func(t *testing.T) {
			decodedLog := &parser.DecodedLog{
				LogIndex: 0,
				Address:  "log-address",
				Arguments: []parser.Argument{
					{
						Name:    "arg1",
						Type:    "string",
						Value:   "some-value",
						Indexed: true,
					},
				},
				EventName: "SomeEvent",
				OutputData: map[string]interface{}{
					"output": "data",
				},
			}

			txLog := &storage.TransactionLog{
				TransactionHash:  insertedTransactions[0].TransactionHash,
				TransactionIndex: insertedTransactions[0].TransactionIndex,
				BlockNumber:      insertedTransactions[0].BlockNumber,
			}

			_, err := blockStore.InsertTransactionLog(
				txLog.TransactionHash,
				txLog.TransactionIndex,
				txLog.BlockNumber,
				decodedLog,
				decodedLog.OutputData,
			)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "duplicate key value violates unique constraint")
		})
	})

	t.Cleanup(func() {
		teardown(dbname, cfg, db, l)
	})
}
