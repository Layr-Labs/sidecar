package postgres

import (
	"strings"
	"testing"
	"time"

	"os"

	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"

	"github.com/Layr-Labs/sidecar/internal/config"
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
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
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
				Number:     100,
				Hash:       "some hash",
				ParentHash: "parent hash",
				BlockTime:  time.Now(),
			}

			insertedBlock, err := blockStore.InsertBlockAtHeight(block.Number, block.Hash, block.ParentHash, uint64(block.BlockTime.Unix()))
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
				Number:     100,
				Hash:       "some hash",
				ParentHash: "parent hash",
				BlockTime:  time.Now(),
			}

			_, err := blockStore.InsertBlockAtHeight(block.Number, block.Hash, block.ParentHash, uint64(block.BlockTime.Unix()))
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "duplicate key value violates unique constraint")
		})
	})
	t.Run("Transactions", func(t *testing.T) {
		block := insertedBlocks[0]

		t.Run("InsertBlockTransaction", func(t *testing.T) {
			tx := storage.Transaction{
				BlockNumber:       block.Number,
				TransactionHash:   "txHash",
				TransactionIndex:  0,
				FromAddress:       "from",
				ToAddress:         "to",
				ContractAddress:   "contractAddress",
				BytecodeHash:      "bytecodeHash",
				GasUsed:           1000000,
				CumulativeGasUsed: 1000000,
				EffectiveGasPrice: 1000000,
			}
			insertedTx, err := blockStore.InsertBlockTransaction(
				tx.BlockNumber,
				tx.TransactionHash,
				tx.TransactionIndex,
				tx.FromAddress,
				tx.ToAddress,
				tx.ContractAddress,
				tx.BytecodeHash,
				tx.GasUsed,
				tx.CumulativeGasUsed,
				tx.EffectiveGasPrice,
				false,
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
			assert.Equal(t, tx.GasUsed, insertedTx.GasUsed)
			assert.Equal(t, tx.CumulativeGasUsed, insertedTx.CumulativeGasUsed)
			assert.Equal(t, tx.EffectiveGasPrice, insertedTx.EffectiveGasPrice)

			insertedTransactions = append(insertedTransactions, insertedTx)
		})
		t.Run("Fail to insert a duplicate transaction", func(t *testing.T) {
			tx := storage.Transaction{
				BlockNumber:       block.Number,
				TransactionHash:   "txHash",
				TransactionIndex:  0,
				FromAddress:       "from",
				ToAddress:         "to",
				ContractAddress:   "contractAddress",
				BytecodeHash:      "bytecodeHash",
				GasUsed:           1000000,
				CumulativeGasUsed: 1000000,
				EffectiveGasPrice: 1000000,
			}
			_, err := blockStore.InsertBlockTransaction(
				tx.BlockNumber,
				tx.TransactionHash,
				tx.TransactionIndex,
				tx.FromAddress,
				tx.ToAddress,
				tx.ContractAddress,
				tx.BytecodeHash,
				tx.GasUsed,
				tx.CumulativeGasUsed,
				tx.EffectiveGasPrice,
				false,
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
				false,
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
				false,
			)
			assert.NotNil(t, err)
			assert.Contains(t, err.Error(), "duplicate key value violates unique constraint")
		})
	})

	t.Cleanup(func() {
		teardown(dbname, cfg, db, l)
	})
}

func TestSanitizeNullBytes(t *testing.T) {
	t.Run("should remove null bytes from strings", func(t *testing.T) {
		input := "Catalysis slashing for committee: \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01"
		expected := "Catalysis slashing for committee: \x01"
		result := sanitizeNullBytes(input)
		assert.Equal(t, expected, result)
	})

	t.Run("should handle strings without null bytes", func(t *testing.T) {
		input := "normal string"
		result := sanitizeNullBytes(input)
		assert.Equal(t, input, result)
	})

	t.Run("should recursively sanitize maps", func(t *testing.T) {
		input := map[string]interface{}{
			"description": "test\x00value",
			"operator":    "0x123",
			"nested": map[string]interface{}{
				"field": "nested\x00\x00data",
			},
		}
		expected := map[string]interface{}{
			"description": "testvalue",
			"operator":    "0x123",
			"nested": map[string]interface{}{
				"field": "nesteddata",
			},
		}
		result := sanitizeNullBytes(input)
		assert.Equal(t, expected, result)
	})

	t.Run("should recursively sanitize slices", func(t *testing.T) {
		input := []interface{}{
			"string\x00with\x00nulls",
			123,
			[]interface{}{
				"nested\x00array",
			},
		}
		expected := []interface{}{
			"stringwithnulls",
			123,
			[]interface{}{
				"nestedarray",
			},
		}
		result := sanitizeNullBytes(input)
		assert.Equal(t, expected, result)
	})

	t.Run("should preserve non-string types", func(t *testing.T) {
		input := map[string]interface{}{
			"number":  123,
			"boolean": true,
			"null":    nil,
			"float":   3.14,
		}
		result := sanitizeNullBytes(input)
		assert.Equal(t, input, result)
	})

	t.Run("should handle complex nested structures", func(t *testing.T) {
		input := map[string]interface{}{
			"operator":    "0xce28d7e5a7928a56379490026c1ec595edbf12e4",
			"description": "Catalysis slashing for committee: \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01",
			"operatorSet": map[string]interface{}{
				"avs": "0x14b1e4b038490f903b297edb668cb639b708555e",
				"id":  1,
			},
			"strategies": []interface{}{"0xe0cac550f1915a28a098cc7d24035e85598c0db5"},
			"wadSlashed": []interface{}{0},
		}

		result := sanitizeNullBytes(input).(map[string]interface{})

		// Verify description is sanitized
		assert.Equal(t, "Catalysis slashing for committee: \x01", result["description"])

		// Verify other fields are unchanged
		assert.Equal(t, "0xce28d7e5a7928a56379490026c1ec595edbf12e4", result["operator"])
		assert.Equal(t, 1, result["operatorSet"].(map[string]interface{})["id"])
	})
}
