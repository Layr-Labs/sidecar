package transactionBackfiller

import (
	"context"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	pgStorage "github.com/Layr-Labs/sidecar/pkg/storage/postgres"
	"github.com/Layr-Labs/sidecar/pkg/transactionLogParser"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func setup(ethConfig *ethereum.EthereumClientConfig) (
	*config.Config,
	*zap.Logger,
	*fetcher.Fetcher,
	storage.BlockStore,
	*gorm.DB,
	*contractManager.ContractManager,
	string,
	error,
) {
	const (
		rpcUrl = "http://185.26.8.67:8545"
	)

	cfg := config.NewConfig()
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.Chain = config.Chain_Mainnet
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	ethConfig.BaseUrl = rpcUrl
	ethConfig.UseNativeBatchCall = true
	ethConfig.NativeBatchCallSize = 10
	client := ethereum.NewClient(ethConfig, l)

	af := abiFetcher.NewAbiFetcher(client, &http.Client{Timeout: 5 * time.Second}, l, []abiSource.AbiSource{})

	metricsClients, err := metrics.InitMetricsSinksFromConfig(cfg, l)
	if err != nil {
		l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
	}

	sink, err := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, metricsClients)
	if err != nil {
		l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
	}

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		log.Fatal(err)
	}

	contractStore := postgresContractStore.NewPostgresContractStore(grm, l, cfg)
	if err := contractStore.InitializeCoreContracts(); err != nil {
		log.Fatalf("Failed to initialize core contracts: %v", err)
	}

	mds := pgStorage.NewPostgresBlockStore(grm, l, cfg)

	cm := contractManager.NewContractManager(grm, contractStore, client, af, sink, l)

	fetchr := fetcher.NewFetcher(client, cfg, l)

	return cfg, l, fetchr, mds, grm, cm, dbname, nil

}

type customLogHandler struct {
}

func (cl *customLogHandler) IsInterestingAddress(address string) bool {
	return true
}

func Test_TransactionBackfiller(t *testing.T) {
	cfg, l, fetcher, mds, grm, cm, dbname, err := setup(&ethereum.EthereumClientConfig{})
	assert.Nil(t, err)

	bf := NewTransactionBackfiller(&TransactionBackfillerConfig{}, l, fetcher, mds)

	t.Run("Test backfill two blocks", func(t *testing.T) {
		logsHandled := atomic.Uint64{}
		logsHandled.Store(0)

		logHandler := &customLogHandler{}

		logParser := transactionLogParser.NewTransactionLogParser(l, cm, logHandler)

		message := &BackfillerMessage{
			StartBlock: 22020900,
			EndBlock:   22020901,
			IsInterestingLog: func(log *ethereum.EthereumEventLog) bool {
				return strings.ToLower(log.Address.Value()) == cfg.GetContractsMapForChain().RewardsCoordinator
			},
			TransactionLogHandler: func(block *ethereum.EthereumBlock, receipt *ethereum.EthereumTransactionReceipt, log *ethereum.EthereumEventLog) error {
				fmt.Printf("Handling log: %+v\n", receipt)
				logsHandled.Add(1)

				decodedLog, err := logParser.DecodeLogWithAbi(nil, receipt, log)
				if err != nil {
					return err
				}
				fmt.Printf("Decoded log: %+v\n", decodedLog)
				return nil
			},
		}

		go bf.Process()

		res, err := bf.EnqueueAndWait(context.Background(), message)
		fmt.Printf("Error: %v\n", err)
		fmt.Printf("Response: %v\n", res)
		assert.Equal(t, uint64(1), logsHandled.Load())

		bf.Close()
	})
	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbname, cfg, grm, l)
	})
}
