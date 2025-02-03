package pipeline

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller/sequentialContractCaller"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/eigenState"
	"github.com/Layr-Labs/sidecar/pkg/eventBus"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/indexer"
	"github.com/Layr-Labs/sidecar/pkg/metaState"
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
	"github.com/Layr-Labs/sidecar/pkg/eigenState/avsOperators"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup(ethConfig *ethereum.EthereumClientConfig) (
	*fetcher.Fetcher,
	*indexer.Indexer,
	storage.BlockStore,
	contractStore.ContractStore,
	*contractManager.ContractManager,
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

	ethConfig.BaseUrl = rpcUrl
	client := ethereum.NewClient(ethConfig, l)

	af := abiFetcher.NewAbiFetcher(client, abiFetcher.DefaultHttpClient(), l, cfg, []abiSource.AbiSource{})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		log.Fatal(err)
	}

	contractStore := postgresContractStore.NewPostgresContractStore(grm, l, cfg)
	if err := contractStore.InitializeCoreContracts(); err != nil {
		log.Fatalf("Failed to initialize core contracts: %v", err)
	}

	cm := contractManager.NewContractManager(grm, contractStore, client, af, sdc, l, cfg)

	mds := pgStorage.NewPostgresBlockStore(grm, l, cfg)

	sm := stateManager.NewEigenStateManager(nil, l, grm)
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

	return fetchr, idxr, mds, contractStore, cm, sm, msm, rc, rcq, cfg, l, sdc, grm, eb, dbname

}

func Test_PipelineIntegration(t *testing.T) {

	t.Run("Should index a block, transaction with logs using native batched ethereum client", func(t *testing.T) {
		ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
		fetchr, idxr, mds, contractStore, cm, sm, msm, rc, rcq, cfg, l, sdc, grm, eb, dbName := setup(ethConfig)
		blockNumber := uint64(20386320)

		p := NewPipeline(fetchr, idxr, mds, contractStore, cm, sm, msm, rc, rcq, cfg, sdc, eb, l)

		err := p.RunForBlockBatch(context.Background(), blockNumber, blockNumber+1, true)
		assert.Nil(t, err)

		query := `select * from avs_operator_state_changes where block_number = @blockNumber`
		avsOperatorChanges := make([]avsOperators.AvsOperatorStateChange, 0)
		res := grm.Raw(query, sql.Named("blockNumber", blockNumber)).Scan(&avsOperatorChanges)
		assert.Nil(t, res.Error)

		for _, change := range avsOperatorChanges {
			fmt.Printf("Change: %+v\n", change)
		}

		assert.Equal(t, 1, len(avsOperatorChanges))
		assert.Equal(t, "0xf6ad76de4c80c056a51fcb457942df40a6d99f76", avsOperatorChanges[0].Operator)
		assert.Equal(t, "0xe7d0894ac9266f5cbe8f8e750ac6cbe128fbbeb7", avsOperatorChanges[0].Avs)
		assert.Equal(t, uint64(128), avsOperatorChanges[0].LogIndex)
		assert.Equal(t, blockNumber, avsOperatorChanges[0].BlockNumber)

		t.Cleanup(func() {
			postgres.TeardownTestDatabase(dbName, cfg, grm, l)
		})
	})
	t.Run("Should index a block, transaction with logs using chunked ethereum client", func(t *testing.T) {
		ethConfig := ethereum.DefaultChunkedCallEthereumClientConfig()
		fetchr, idxr, mds, contractStore, cm, sm, msm, rc, rcq, cfg, l, sdc, grm, eb, dbName := setup(ethConfig)
		blockNumber := uint64(20386320)

		p := NewPipeline(fetchr, idxr, mds, contractStore, cm, sm, msm, rc, rcq, cfg, sdc, eb, l)

		err := p.RunForBlockBatch(context.Background(), blockNumber, blockNumber+1, true)
		assert.Nil(t, err)

		query := `select * from avs_operator_state_changes where block_number = @blockNumber`
		avsOperatorChanges := make([]avsOperators.AvsOperatorStateChange, 0)
		res := grm.Raw(query, sql.Named("blockNumber", blockNumber)).Scan(&avsOperatorChanges)
		assert.Nil(t, res.Error)

		for _, change := range avsOperatorChanges {
			fmt.Printf("Change: %+v\n", change)
		}

		assert.Equal(t, 1, len(avsOperatorChanges))
		assert.Equal(t, "0xf6ad76de4c80c056a51fcb457942df40a6d99f76", avsOperatorChanges[0].Operator)
		assert.Equal(t, "0xe7d0894ac9266f5cbe8f8e750ac6cbe128fbbeb7", avsOperatorChanges[0].Avs)
		assert.Equal(t, uint64(128), avsOperatorChanges[0].LogIndex)
		assert.Equal(t, blockNumber, avsOperatorChanges[0].BlockNumber)

		t.Cleanup(func() {
			postgres.TeardownTestDatabase(dbName, cfg, grm, l)
		})
	})
}
