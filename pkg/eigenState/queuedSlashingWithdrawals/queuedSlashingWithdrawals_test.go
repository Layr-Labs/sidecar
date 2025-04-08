package queuedSlashingWithdrawals

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"math/big"
	"os"
	"testing"
	"time"
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

func createBlock(model *QueuedSlashingWithdrawalModel, blockNumber uint64) error {
	block := &storage.Block{
		Number:    blockNumber,
		Hash:      "some hash",
		BlockTime: time.Now().Add(time.Hour * time.Duration(blockNumber)),
	}
	res := model.DB.Model(&storage.Block{}).Create(block)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func Test_QueuedSlashingWithdrawals(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test each event type", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		model, err := NewQueuedSlashingWithdrawalModel(esm, grm, l, cfg)

		t.Run("Handle an QueuedSlashingWithdrawalUpdated", func(t *testing.T) {
			blockNumber := uint64(102)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().DelegationManager,
				Arguments:        `[{"Name": "withdrawalRoot", "Type": "bytes32", "Value": null, "Indexed": false}, {"Name": "withdrawal", "Type": "(address,address,address,uint256,uint32,address[],uint256[])", "Value": null, "Indexed": false}, {"Name": "sharesToWithdraw", "Type": "uint256[]", "Value": null, "Indexed": false}]`,
				EventName:        "SlashingWithdrawalQueued",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"withdrawal": {"nonce": 35, "staker": "0xef6c37c1129ca9c5970805b170889d9a13594eab", "startBlock": 2970192, "strategies": ["0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0"], "withdrawer": "0xef6c37c1129ca9c5970805b170889d9a13594eab", "delegatedTo": "0x501808a186cc1edc95c9f5722db76b4470e0b12a", "scaledShares": [31749936128000000000]}, "withdrawalRoot": [114, 71, 98, 0, 207, 56, 75, 84, 102, 71, 123, 58, 247, 58, 203, 38, 235, 5, 207, 158, 255, 1, 37, 16, 129, 164, 42, 1, 62, 203, 13, 197], "sharesToWithdraw": [31749936128000000000]}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			records := change.([]*QueuedSlashingWithdrawal)

			assert.Equal(t, 1, len(records))

			assert.Equal(t, "0xef6c37c1129ca9c5970805b170889d9a13594eab", records[0].Staker)
			assert.Equal(t, "0x501808a186cc1edc95c9f5722db76b4470e0b12a", records[0].Operator)
			assert.Equal(t, "0xef6c37c1129ca9c5970805b170889d9a13594eab", records[0].Withdrawer)
			assert.Equal(t, uint64(2970192), records[0].StartBlock)
			assert.Equal(t, "35", records[0].Nonce)
			assert.Equal(t, "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", records[0].Strategy)
			assert.Equal(t, "31749936128000000000", records[0].ScaledShares)
			assert.Equal(t, "31749936128000000000", records[0].SharesToWithdraw)
			assert.Equal(t, "72476200cf384b5466477b3af73acb26eb05cf9eff01251081a42a013ecb0dc5", records[0].WithdrawalRoot)
			assert.Equal(t, log.TransactionHash, records[0].TransactionHash)
			assert.Equal(t, log.LogIndex, records[0].LogIndex)
			assert.Equal(t, log.BlockNumber, records[0].BlockNumber)

			err = model.CommitFinalState(blockNumber, false)
			assert.Nil(t, err)

			results := make([]*QueuedSlashingWithdrawal, 0)
			query := `select * from queued_slashing_withdrawals where block_number = ?`
			res := model.DB.Raw(query, blockNumber).Scan(&results)
			assert.Nil(t, res.Error)
			assert.Equal(t, 1, len(results))

			stateRoot, err := model.GenerateStateRoot(blockNumber)
			assert.Nil(t, err)
			assert.NotNil(t, stateRoot)
			assert.True(t, len(stateRoot) > 0)
		})
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
