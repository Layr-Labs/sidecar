package _202505092016_fixRewardsClaimedTransactions

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller/sequentialContractCaller"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/eigenState"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/indexer"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metaState"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	pgStorage "github.com/Layr-Labs/sidecar/pkg/storage/postgres"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup(ethConfig *ethereum.EthereumClientConfig) (
	*fetcher.Fetcher,
	*indexer.Indexer,
	*ethereum.Client,
	storage.BlockStore,
	contractStore.ContractStore,
	*contractManager.ContractManager,
	*config.Config,
	*zap.Logger,
	*gorm.DB,
	string,
) {
	const (
		rpcUrl = "https://holesky.drpc.org"
	)

	cfg := config.NewConfig()
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.Chain = config.Chain_Holesky
	cfg.EthereumRpcConfig.BaseUrl = rpcUrl
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	ethConfig.BaseUrl = rpcUrl
	client := ethereum.NewClient(ethConfig, l)

	af := abiFetcher.NewAbiFetcher(client, abiFetcher.DefaultHttpClient(), l, []abiSource.AbiSource{})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		log.Fatal(err)
	}

	contractStore := postgresContractStore.NewPostgresContractStore(grm, l)

	cm := contractManager.NewContractManager(grm, contractStore, client, af, l)

	mds := pgStorage.NewPostgresBlockStore(grm, l, cfg)

	sm := stateManager.NewEigenStateManager(nil, l, grm)
	if err := eigenState.LoadEigenStateModels(sm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to load eigen state models", zap.Error(err))
	}

	msm := metaStateManager.NewMetaStateManager(grm, l, cfg)
	if err := metaState.LoadMetaStateModels(msm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to load meta state models", zap.Error(err))
	}

	fetchr := fetcher.NewFetcher(client, &fetcher.FetcherConfig{UseGetBlockReceipts: cfg.EthereumRpcConfig.UseGetBlockReceipts}, contractStore, l)

	cc := sequentialContractCaller.NewSequentialContractCaller(client, cfg, 10, l)

	idxr := indexer.NewIndexer(mds, contractStore, cm, client, fetchr, cc, grm, l, cfg)

	return fetchr, idxr, client, mds, contractStore, cm, cfg, l, grm, dbname

}

func hydrateBlocks(grm *gorm.DB, projectRoot string) error {
	blocks, err := tests.GetFixClaimedTransactionsJobBlocksSqlFile(projectRoot)
	if err != nil {
		return err
	}
	res := grm.Exec(blocks)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func hydrateTransactions(grm *gorm.DB, projectRoot string) error {
	transactions, err := tests.GetFixClaimedTransactionsJobTransactionsSqlFile(projectRoot)
	if err != nil {
		return err
	}
	res := grm.Exec(transactions)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func hydrateTransactionLogs(grm *gorm.DB, projectRoot string) error {
	transactionLogs, err := tests.GetFixClaimedTransactionsJobTransactionLogsSqlFile(projectRoot)
	if err != nil {
		return err
	}
	res := grm.Exec(transactionLogs)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func Test_FixRewardsClaimedJob(t *testing.T) {
	projectRoot := tests.GetProjectRoot()

	ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
	fetchr, idxr, ethClient, _, _, _, cfg, l, grm, dbName := setup(ethConfig)
	ctx := context.Background()

	job := &StartupJob{}

	job.Init(ctx, cfg, ethClient, idxr, fetchr, grm, l)

	// setup stuff
	if err := hydrateBlocks(grm, projectRoot); err != nil {
		t.Fatalf("Failed to hydrate blocks: %v", err)
	}
	t.Logf("Hydrated blocks")

	if err := hydrateTransactions(grm, projectRoot); err != nil {
		t.Fatalf("Failed to hydrate transactions: %v", err)
	}
	t.Logf("Hydrated transactions")

	if err := hydrateTransactionLogs(grm, projectRoot); err != nil {
		t.Fatalf("Failed to hydrate transaction logs: %v", err)
	}
	t.Logf("Hydrated transaction logs")

	t.Run("should chunkify", func(t *testing.T) {
		chunks := chunkify([]uint64{1, 2, 3, 4, 5}, 2)
		assert.Equal(t, 3, len(chunks))
		assert.Equal(t, []uint64{1, 2}, chunks[0])
		assert.Equal(t, []uint64{3, 4}, chunks[1])
		assert.Equal(t, []uint64{5}, chunks[2])
	})
	t.Run("should chunkify large numbers", func(t *testing.T) {
		chunks := chunkify([]uint64{1, 2, 99, 300, 700, 703}, 2)
		assert.Equal(t, 4, len(chunks))
		fmt.Printf("Chunks: %+v\n", chunks)
		assert.Equal(t, []uint64{1, 2}, chunks[0])
		assert.Equal(t, []uint64{99}, chunks[1])
		assert.Equal(t, []uint64{300}, chunks[2])
		assert.Equal(t, []uint64{700, 703}, chunks[3])
	})

	t.Run("should list rewards claimed for blocks", func(t *testing.T) {
		blocks, err := job.listRewardsClaimedBlocks()
		assert.Nil(t, err)
		assert.Equal(t, 5521, len(blocks))
	})

	t.Run("should process blocks", func(t *testing.T) {
		blocks, err := fetchBadTransactionLogBlocks(grm, cfg)
		if err != nil {
			t.Fatalf("Failed to fetch blocks: %v", err)
		}

		chunk := []uint64{blocks[0]}
		fmt.Printf("Chunk: %v\n", chunk)

		transactionLogs, err := listTransactionLogs(chunk, grm)
		if err != nil {
			t.Fatalf("Failed to list transaction logs: %v", err)
		}

		err = job.handleChunk(ctx, chunk)
		if err != nil {
			t.Fatalf("Failed to handle chunk: %v", err)
		}
		updatedTransactionLogs, err := listTransactionLogs(chunk, grm)
		if err != nil {
			t.Fatalf("Failed to list updated transaction logs: %v", err)
		}

		for _, log := range transactionLogs {
			updatedLog := utils.Find(updatedTransactionLogs, func(tl *storage.TransactionLog) bool {
				return tl.TransactionIndex == log.TransactionIndex &&
					tl.BlockNumber == log.BlockNumber &&
					tl.LogIndex == log.LogIndex &&
					tl.TransactionHash == log.TransactionHash
			})
			assert.NotNil(t, updatedLog)
			fmt.Printf("\nLog: %d %d - %s\n", log.BlockNumber, log.LogIndex, log.TransactionHash)
			fmt.Printf("Original log arguments: %+v\n", log.Arguments)
			fmt.Printf("Updated log arguments:  %+v\n", updatedLog.Arguments)
		}
	})

	t.Run("should truncate and rehydrate rewards_claimed", func(t *testing.T) {
		if err := job.rehydrateRewardsClaimed(); err != nil {
			t.Fatalf("Failed to rehydrate rewards claimed: %v", err)
		}
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}

func listTransactionLogs(blocks []uint64, grm *gorm.DB) ([]*storage.TransactionLog, error) {
	query := `select * from transaction_logs where block_number in @blockNumbers`
	transactionLogs := make([]*storage.TransactionLog, 0)
	res := grm.Raw(query, sql.Named("blockNumbers", blocks)).Scan(&transactionLogs)
	if res.Error != nil {
		return nil, res.Error
	}
	return transactionLogs, nil
}

func fetchBadTransactionLogBlocks(grm *gorm.DB, cfg *config.Config) ([]uint64, error) {
	query := `
		select
			distinct(block_number)
		from transaction_logs
		where
			address = @rewardsCoordinatorAddress
			and event_name = 'RewardsClaimed'
			and arguments #>> '{3, Value}' is null
	`
	var blocks []uint64
	res := grm.Raw(query, sql.Named("rewardsCoordinatorAddress", cfg.GetContractsMapForChain().RewardsCoordinator)).Scan(&blocks)
	if res.Error != nil {
		return nil, res.Error
	}
	return blocks, nil
}
