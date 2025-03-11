package backfiller

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/metaState"

	"os"

	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller/sequentialContractCaller"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/eigenState"
	"github.com/Layr-Labs/sidecar/pkg/eventBus"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/indexer"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	pgStorage "github.com/Layr-Labs/sidecar/pkg/storage/postgres"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup() (
	*fetcher.Fetcher,
	*indexer.Indexer,
	storage.BlockStore,
	*stateManager.EigenStateManager,
	*metaStateManager.MetaStateManager,
	*rewards.RewardsCalculator,
	*rewardsCalculatorQueue.RewardsCalculatorQueue,
	*config.Config,
	*zap.Logger,
	*metrics.MetricsSink,
	*gorm.DB,
	*eventBus.EventBus,
	string,
) {
	const (
		rpcUrl = "http://72.46.85.253:8545"
	)

	cfg := config.NewConfig()
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.Chain = config.Chain_Mainnet
	cfg.EthereumRpcConfig.BaseUrl = rpcUrl
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	metricsClients, err := metrics.InitMetricsSinksFromConfig(cfg, l)
	if err != nil {
		l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
	}

	sdc, err := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, metricsClients)
	if err != nil {
		l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
	}

	ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
	ethConfig.BaseUrl = rpcUrl
	client := ethereum.NewClient(ethConfig, l)

	af := abiFetcher.NewAbiFetcher(client, &http.Client{Timeout: 5 * time.Second}, l, cfg, []abiSource.AbiSource{})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		log.Fatal(err)
	}

	contractStore := postgresContractStore.NewPostgresContractStore(grm, l, cfg)
	if err := contractStore.InitializeCoreContracts(); err != nil {
		log.Fatalf("Failed to initialize core contracts: %v", err)
	}

	cm := contractManager.NewContractManager(contractStore, client, af, sdc, l)

	mds := pgStorage.NewPostgresBlockStore(grm, l, cfg)

	sm := stateManager.NewEigenStateManager(l, grm)
	if err := eigenState.LoadEigenStateModels(sm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to load eigen state models", zap.Error(err))
	}

	msm := metaStateManager.NewMetaStateManager(grm, l, cfg)
	if err := metaState.LoadMetaStateModels(msm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to load meta state models", zap.Error(err))
	}

	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, _ := rewards.NewRewardsCalculator(cfg, grm, mds, sog, sdc, l)

	rcq := rewardsCalculatorQueue.NewRewardsCalculatorQueue(rc, l)

	fetchr := fetcher.NewFetcher(client, cfg, l)

	cc := sequentialContractCaller.NewSequentialContractCaller(client, cfg, 10, l)

	idxr := indexer.NewIndexer(mds, contractStore, cm, client, fetchr, cc, grm, l, cfg)

	eb := eventBus.NewEventBus(l)

	return fetchr, idxr, mds, sm, msm, rc, rcq, cfg, l, sdc, grm, eb, dbname
}

func Test_Backfiller(t *testing.T) {
	fetchr, idxr, _, _, _, _, _, cfg, l, _, _, _, _ := setup()

	baseUrl := "http://72.46.85.253:8545"
	ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
	ethConfig.BaseUrl = baseUrl

	client := ethereum.NewClient(ethConfig, l)

	b := NewBackfiller(client, idxr, fetchr, cfg, l)
	t.Run("Test getting transaction receipts", func(t *testing.T) {
		receipts, _ := b.Backfill(context.Background(), 19592323, []string{""})
		for _, receipt := range receipts {
			fmt.Println(receipt.TransactionHash, receipt.GetTargetAddress())
		}
		assert.Equal(t, 1, len(receipts))
	})
}
