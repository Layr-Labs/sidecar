package operatorAllocations

import (
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
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

func createBlock(model *OperatorAllocationModel, blockNumber uint64) error {
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

func Test_OperatorAllocations(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test each event type", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		model, err := NewOperatorAllocationModel(esm, grm, l, cfg)

		t.Run("Handle an allocationUpdated", func(t *testing.T) {
			blockNumber := uint64(102)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().AllocationManager,
				Arguments:        `[{"Name": "operator", "Type": "address", "Value": null, "Indexed": false}, {"Name": "operatorSet", "Type": "(address,uint32)", "Value": null, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": null, "Indexed": false}, {"Name": "magnitude", "Type": "uint64", "Value": null, "Indexed": false}, {"Name": "effectBlock", "Type": "uint32", "Value": null, "Indexed": false}]`,
				EventName:        "AllocationUpdated",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"operator": "0x93a797473810c125ece22f25a2087b6ceb8ce886", "strategy": "0x947e522010e22856071f8fb03e735fedfccd6e9f", "magnitude": 400000000000000000, "effectBlock": 2937866, "operatorSet": {"id": 5, "avs": "0x69aa865947f6c9191b02954b1dd1a44131541226"}}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			record := change.(*OperatorAllocation)

			assert.Equal(t, strings.ToLower("0x93a797473810c125ece22f25a2087b6ceb8ce886"), record.Operator)
			assert.Equal(t, strings.ToLower("0x947e522010e22856071f8fb03e735fedfccd6e9f"), record.Strategy)
			assert.Equal(t, "400000000000000000", record.Magnitude)
			assert.Equal(t, uint64(2937866), record.EffectiveBlock)
			assert.Equal(t, uint64(5), record.OperatorSetId)
			assert.Equal(t, strings.ToLower("0x69aa865947f6c9191b02954b1dd1a44131541226"), record.Avs)

			err = model.CommitFinalState(blockNumber, false)
			assert.Nil(t, err)

			results := make([]*OperatorAllocation, 0)
			query := `select * from operator_allocations where block_number = ?`
			res := model.DB.Raw(query, blockNumber).Scan(&results)
			assert.Nil(t, res.Error)
			assert.Equal(t, 1, len(results))

			stateRoot, err := model.GenerateStateRoot(blockNumber)
			assert.Nil(t, err)
			assert.NotNil(t, stateRoot)
			assert.True(t, len(stateRoot) > 0)
		})
	})

	t.Run("Test determineEffectiveDate logic", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		model, err := NewOperatorAllocationModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		timestamp := time.Date(2025, 11, 14, 15, 30, 0, 0, time.UTC)

		t.Run("Allocation (increase) - should round up to next day", func(t *testing.T) {
			previousMagnitude := big.NewInt(1000)
			newMagnitude := big.NewInt(1500) // Increase of 500

			result := model.determineEffectiveDate(timestamp, newMagnitude, previousMagnitude)
			expected := time.Date(2025, 11, 15, 0, 0, 0, 0, time.UTC)
			assert.Equal(t, expected, result)
		})

		t.Run("Deallocation (decrease) - should round down to current day", func(t *testing.T) {
			previousMagnitude := big.NewInt(1500)
			newMagnitude := big.NewInt(1000) // Decrease of 500

			result := model.determineEffectiveDate(timestamp, newMagnitude, previousMagnitude)
			expected := time.Date(2025, 11, 14, 0, 0, 0, 0, time.UTC)
			assert.Equal(t, expected, result)
		})

		t.Run("No change in magnitude - should round down to current day", func(t *testing.T) {
			previousMagnitude := big.NewInt(1000)
			newMagnitude := big.NewInt(1000) // No change

			result := model.determineEffectiveDate(timestamp, newMagnitude, previousMagnitude)
			expected := time.Date(2025, 11, 14, 0, 0, 0, 0, time.UTC)
			assert.Equal(t, expected, result)
		})

		t.Run("First allocation (from zero) - should round up to next day", func(t *testing.T) {
			previousMagnitude := big.NewInt(0)
			newMagnitude := big.NewInt(1000) // First allocation

			result := model.determineEffectiveDate(timestamp, newMagnitude, previousMagnitude)
			expected := time.Date(2025, 11, 15, 0, 0, 0, 0, time.UTC)
			assert.Equal(t, expected, result)
		})

		t.Run("Full deallocation (to zero) - should round down to current day", func(t *testing.T) {
			previousMagnitude := big.NewInt(1000)
			newMagnitude := big.NewInt(0) // Full deallocation

			result := model.determineEffectiveDate(timestamp, newMagnitude, previousMagnitude)
			expected := time.Date(2025, 11, 14, 0, 0, 0, 0, time.UTC)
			assert.Equal(t, expected, result)
		})
	})

	t.Run("Test allocation vs deallocation integration", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		model, err := NewOperatorAllocationModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		// Clean up any existing test data to ensure isolation
		err = grm.Exec("DELETE FROM operator_allocations").Error
		assert.Nil(t, err)

		operator := "0x93a797473810c125ece22f25a2087b6ceb8ce886"
		avs := "0x69aa865947f6c9191b02954b1dd1a44131541226"
		strategy := "0x947e522010e22856071f8fb03e735fedfccd6e9f"

		t.Run("First allocation event", func(t *testing.T) {
			blockNumber := uint64(200)
			blockTime := time.Date(2025, 11, 14, 15, 30, 0, 0, time.UTC)

			block := &storage.Block{
				Number:    blockNumber,
				Hash:      "hash_200",
				BlockTime: blockTime,
			}
			res := model.DB.Model(&storage.Block{}).Create(block)
			assert.Nil(t, res.Error)

			log := &storage.TransactionLog{
				TransactionHash:  "tx_hash_200",
				TransactionIndex: 1,
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().AllocationManager,
				Arguments:        `[{"Name": "operator", "Type": "address", "Value": null, "Indexed": false}]`,
				EventName:        "AllocationUpdated",
				LogIndex:         1,
				OutputData:       `{"operator": "` + operator + `", "strategy": "` + strategy + `", "magnitude": 1000, "effectBlock": 200, "operatorSet": {"id": 5, "avs": "` + avs + `"}}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			record := change.(*OperatorAllocation)

			// First allocation should round UP to next day
			assert.Equal(t, "2025-11-15", record.EffectiveDate)
			assert.Equal(t, "1000", record.Magnitude)

			err = model.CommitFinalState(blockNumber, false)
			assert.Nil(t, err)
		})

		t.Run("Second allocation event (increase)", func(t *testing.T) {
			blockNumber := uint64(201)
			blockTime := time.Date(2025, 11, 15, 10, 0, 0, 0, time.UTC)

			block := &storage.Block{
				Number:    blockNumber,
				Hash:      "hash_201",
				BlockTime: blockTime,
			}
			res := model.DB.Model(&storage.Block{}).Create(block)
			assert.Nil(t, res.Error)

			log := &storage.TransactionLog{
				TransactionHash:  "tx_hash_201",
				TransactionIndex: 1,
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().AllocationManager,
				Arguments:        `[{"Name": "operator", "Type": "address", "Value": null, "Indexed": false}]`,
				EventName:        "AllocationUpdated",
				LogIndex:         1,
				OutputData:       `{"operator": "` + operator + `", "strategy": "` + strategy + `", "magnitude": 1500, "effectBlock": 201, "operatorSet": {"id": 5, "avs": "` + avs + `"}}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			record := change.(*OperatorAllocation)

			// Allocation increase should round UP to next day
			assert.Equal(t, "2025-11-16", record.EffectiveDate)
			assert.Equal(t, "1500", record.Magnitude)

			err = model.CommitFinalState(blockNumber, false)
			assert.Nil(t, err)
		})

		t.Run("Deallocation event (decrease)", func(t *testing.T) {
			blockNumber := uint64(202)
			blockTime := time.Date(2025, 11, 16, 14, 0, 0, 0, time.UTC)

			block := &storage.Block{
				Number:    blockNumber,
				Hash:      "hash_202",
				BlockTime: blockTime,
			}
			res := model.DB.Model(&storage.Block{}).Create(block)
			assert.Nil(t, res.Error)

			log := &storage.TransactionLog{
				TransactionHash:  "tx_hash_202",
				TransactionIndex: 1,
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().AllocationManager,
				Arguments:        `[{"Name": "operator", "Type": "address", "Value": null, "Indexed": false}]`,
				EventName:        "AllocationUpdated",
				LogIndex:         1,
				OutputData:       `{"operator": "` + operator + `", "strategy": "` + strategy + `", "magnitude": 500, "effectBlock": 202, "operatorSet": {"id": 5, "avs": "` + avs + `"}}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			record := change.(*OperatorAllocation)

			// Deallocation decrease should round DOWN to current day
			assert.Equal(t, "2025-11-16", record.EffectiveDate)
			assert.Equal(t, "500", record.Magnitude)

			err = model.CommitFinalState(blockNumber, false)
			assert.Nil(t, err)
		})
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
