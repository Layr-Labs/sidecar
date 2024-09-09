package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/clients/ethereum"
	"github.com/Layr-Labs/sidecar/internal/clients/etherscan"
	"github.com/Layr-Labs/sidecar/internal/contractManager"
	"github.com/Layr-Labs/sidecar/internal/contractStore/sqliteContractStore"
	"github.com/Layr-Labs/sidecar/internal/eigenState/avsOperators"
	"github.com/Layr-Labs/sidecar/internal/eigenState/operatorShares"
	"github.com/Layr-Labs/sidecar/internal/eigenState/stakerDelegations"
	"github.com/Layr-Labs/sidecar/internal/eigenState/stakerShares"
	"github.com/Layr-Labs/sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/internal/fetcher"
	"github.com/Layr-Labs/sidecar/internal/indexer"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/internal/sqlite/migrations"
	"github.com/Layr-Labs/sidecar/internal/storage"
	sqliteBlockStore "github.com/Layr-Labs/sidecar/internal/storage/sqlite"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"log"
	"os"
	"testing"
)

var (
	previousEnv = make(map[string]string)
)

func replaceEnv() {
	newEnvValues := map[string]string{
		"SIDECAR_ENVIRONMENT":           "testnet",
		"SIDECAR_NETWORK":               "holesky",
		"SIDECAR_ETHEREUM_RPC_BASE_URL": "http://54.198.82.217:8545",
		"SIDECAR_ETHERSCAN_API_KEYS":    "QIPXW3YCXPR5NQ9GXTRQ3TSXB9EKMGDE34",
		"SIDECAR_STATSD_URL":            "localhost:8125",
		"SIDECAR_DEBUG":                 "true",
	}

	for k, v := range newEnvValues {
		previousEnv[k] = os.Getenv(k)
		os.Setenv(k, v)
	}
}

func restoreEnv() {
	for k, v := range previousEnv {
		os.Setenv(k, v)
	}
}

func setup() (
	*fetcher.Fetcher,
	*indexer.Indexer,
	storage.BlockStore,
	*stateManager.EigenStateManager,
	*zap.Logger,
	*gorm.DB,
) {
	cfg := tests.GetConfig()
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	sdc, err := metrics.InitStatsdClient(cfg.StatsdUrl)
	if err != nil {
		l.Sugar().Fatal("Failed to setup statsd client", zap.Error(err))
	}

	etherscanClient := etherscan.NewEtherscanClient(cfg, l)
	client := ethereum.NewClient(cfg.EthereumRpcConfig.BaseUrl, l)

	// database
	grm, err := tests.GetSqliteDatabaseConnection()
	if err != nil {
		panic(err)
	}
	sqliteMigrator := migrations.NewSqliteMigrator(grm, l)
	if err := sqliteMigrator.MigrateAll(); err != nil {
		l.Sugar().Fatalw("Failed to migrate", "error", err)
	}

	contractStore := sqliteContractStore.NewSqliteContractStore(grm, l, cfg)
	if err := contractStore.InitializeCoreContracts(); err != nil {
		log.Fatalf("Failed to initialize core contracts: %v", err)
	}

	cm := contractManager.NewContractManager(contractStore, etherscanClient, client, sdc, l)

	mds := sqliteBlockStore.NewSqliteBlockStore(grm, l, cfg)
	if err != nil {
		log.Fatalln(err)
	}

	sm := stateManager.NewEigenStateManager(l, grm)

	if _, err := avsOperators.NewAvsOperators(sm, grm, cfg.Network, cfg.Environment, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create AvsOperatorsModel", zap.Error(err))
	}
	if _, err := operatorShares.NewOperatorSharesModel(sm, grm, cfg.Network, cfg.Environment, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create OperatorSharesModel", zap.Error(err))
	}
	if _, err := stakerDelegations.NewStakerDelegationsModel(sm, grm, cfg.Network, cfg.Environment, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create StakerDelegationsModel", zap.Error(err))
	}
	if _, err := stakerShares.NewStakerSharesModel(sm, grm, cfg.Network, cfg.Environment, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create StakerSharesModel", zap.Error(err))
	}

	fetchr := fetcher.NewFetcher(client, cfg, l)

	idxr := indexer.NewIndexer(mds, contractStore, etherscanClient, cm, client, fetchr, l, cfg)

	return fetchr, idxr, mds, sm, l, grm
}

func Test_Pipeline_Integration(t *testing.T) {
	replaceEnv()

	fetchr, idxr, mds, sm, l, grm := setup()
	t.Run("Should create a new Pipeline", func(t *testing.T) {
		p := NewPipeline(fetchr, idxr, mds, sm, l)
		assert.NotNil(t, p)
	})

	t.Run("Should index a block, transaction with logs", func(t *testing.T) {
		blockNumber := uint64(1175560)
		transactionHash := "0x78cc56f0700e7ba5055f124243e6591fc1199cf3c75a17d50f8ea438254c9a76"
		logIndex := uint64(14)

		fmt.Printf("transactionHash: %s %d\n", transactionHash, logIndex)

		p := NewPipeline(fetchr, idxr, mds, sm, l)

		err := p.RunForBlock(context.Background(), blockNumber)
		assert.Nil(t, err)

		query := `select * from delegated_stakers where block_number = @blockNumber`
		delegatedStakers := make([]stakerDelegations.DelegatedStakers, 0)
		res := grm.Raw(query, sql.Named("blockNumber", blockNumber)).Scan(&delegatedStakers)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(delegatedStakers))
	})

	t.Cleanup(func() {
		restoreEnv()
	})
}