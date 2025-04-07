package stakerShares

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func getProjectRootPath() string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	p, err := filepath.Abs(fmt.Sprintf("%s/../../..", wd))
	if err != nil {
		panic(err)
	}
	return p
}

func hydrateAllBlocksTable(grm *gorm.DB, l *zap.Logger) (int, error) {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetAllBlocksSqlFile(projectRoot)

	if err != nil {
		return 0, err
	}

	count := len(strings.Split(strings.Trim(contents, "\n"), "\n")) - 1

	res := grm.Exec(contents)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error))
		return count, res.Error
	}
	return count, nil
}

func hydrateStakerShareTransactionLogs(grm *gorm.DB, l *zap.Logger) error {
	projectRoot := getProjectRootPath()
	contents, err := tests.GetStakerSharesTransactionLogsSqlFile(projectRoot)

	if err != nil {
		return err
	}

	res := grm.Exec(contents)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to execute sql", "error", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

type blockRange struct {
	MinBlockNumber uint64
	MaxBlockNumber uint64
}

func Test_StakerSharesIntegration(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Staker shares calculation through M1 and M2 migration", func(t *testing.T) {
		if !tests.LargeTestsEnabled() {
			t.Skipf("Skipping large test")
		}
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		_, err = hydrateAllBlocksTable(grm, l)
		assert.Nil(t, err)
		t.Logf("Hydrated all blocks table")

		err = hydrateStakerShareTransactionLogs(grm, l)
		assert.Nil(t, err)
		t.Logf("Hydrated staker share transaction logs")

		// get min/max block number
		var minMaxBlocks blockRange
		query := `select min(block_number) as min_block_number, max(block_number) as max_block_number from transaction_logs`
		res := grm.Raw(query).Scan(&minMaxBlocks)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		t.Logf("min block number: %d, max block number: %d", minMaxBlocks.MinBlockNumber, minMaxBlocks.MaxBlockNumber)
		t.Logf("Total blocks to process: %d", minMaxBlocks.MaxBlockNumber-minMaxBlocks.MinBlockNumber+1)

		completed := 0
		for i := minMaxBlocks.MinBlockNumber; i <= minMaxBlocks.MaxBlockNumber; i++ {
			if err = model.SetupStateForBlock(i); err != nil {
				t.Logf("Failed to setup state for block %d", i)
				t.Fatal(err)
			}

			var logs []*storage.TransactionLog
			query := `select * from transaction_logs where block_number = ? order by log_index asc`
			res := grm.Raw(query, i).Scan(&logs)
			if res.Error != nil {
				t.Logf("Failed to get transaction logs for block %d", i)
				t.Fatal(res.Error)
			}

			for _, log := range logs {
				if _, err = model.HandleStateChange(log); err != nil {
					t.Logf("Failed to handle state change for block %d", i)
					t.Fatal(err)
				}
			}

			if err := model.CommitFinalState(i, false); err != nil {
				t.Logf("Failed to commit final state for block %d", i)
				t.Fatal(err)
			}

			if err := model.CleanupProcessedStateForBlock(i); err != nil {
				t.Logf("Failed to cleanup processed state for block %d", i)
				t.Fatal(err)
			}
			completed++
			if completed%50000 == 0 {
				remaining := minMaxBlocks.MaxBlockNumber - minMaxBlocks.MinBlockNumber + 1 - uint64(completed)
				t.Logf("Completed processing %d blocks, remaining: %d", completed, remaining)
			}
		}
		t.Logf("Completed processing all blocks")

		var count int
		query = `select count(*) from staker_share_deltas`
		res = grm.Raw(query).Scan(&count)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		fmt.Printf("Total staker share deltas: %d\n", count)
		assert.Equal(t, 166160, count)
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
