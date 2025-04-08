package completedSlashingWithdrawals

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

func createBlock(model *CompletedSlashingWithdrawalModel, blockNumber uint64) error {
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

func Test_CompletedSlashingWithdrawals(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test each event type", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		model, err := NewCompletedSlashingWithdrawalModel(esm, grm, l, cfg)

		t.Run("Handle an CompletedSlashingWithdrawalUpdated", func(t *testing.T) {
			blockNumber := uint64(102)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().DelegationManager,
				Arguments:        `[{"Name": "withdrawalRoot", "Type": "bytes32", "Value": null, "Indexed": false}]`,
				EventName:        "SlashingWithdrawalCompleted",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"withdrawalRoot": [69, 152, 68, 90, 111, 64, 145, 214, 72, 40, 104, 65, 201, 62, 60, 75, 25, 174, 241, 36, 67, 50, 240, 40, 225, 255, 119, 9, 75, 251, 208, 167]}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			record := change.(*CompletedSlashingWithdrawal)

			assert.Equal(t, "4598445a6f4091d648286841c93e3c4b19aef1244332f028e1ff77094bfbd0a7", record.WithdrawalRoot)
			assert.Equal(t, log.TransactionHash, record.TransactionHash)
			assert.Equal(t, log.LogIndex, record.LogIndex)
			assert.Equal(t, log.BlockNumber, record.BlockNumber)

			err = model.CommitFinalState(blockNumber, false)
			assert.Nil(t, err)

			results := make([]*CompletedSlashingWithdrawal, 0)
			query := `select * from completed_slashing_withdrawals where block_number = ?`
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
