package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"log"

	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	sidecarClient "github.com/Layr-Labs/sidecar/pkg/clients/sidecar"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller/sequentialContractCaller"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/eigenState"
	"github.com/Layr-Labs/sidecar/pkg/eventBus"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/indexer"
	"github.com/Layr-Labs/sidecar/pkg/metaState"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/pipeline"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/proofs"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/rpcServer"
	"github.com/Layr-Labs/sidecar/pkg/service/backfillerService"
	"github.com/Layr-Labs/sidecar/pkg/service/protocolDataService"
	"github.com/Layr-Labs/sidecar/pkg/service/rewardsDataService"
	"github.com/Layr-Labs/sidecar/pkg/sidecar"
	pgStorage "github.com/Layr-Labs/sidecar/pkg/storage/postgres"
	"github.com/Layr-Labs/sidecar/pkg/transactionBackfiller"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/postgres/migrations"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()
	cfg := config.NewConfig()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	eb := eventBus.NewEventBus(l)

	metricsClients, err := metrics.InitMetricsSinksFromConfig(cfg, l)
	if err != nil {
		l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
	}

	sdc, err := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, metricsClients)
	if err != nil {
		l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
	}

	client := ethereum.NewClient(ethereum.ConvertGlobalConfigToEthereumConfig(&cfg.EthereumRpcConfig), l)

	af := abiFetcher.NewAbiFetcher(client, &http.Client{Timeout: 5 * time.Second}, l, cfg, []abiSource.AbiSource{})

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

	pg, err := postgres.NewPostgres(pgConfig)
	if err != nil {
		l.Fatal("Failed to setup postgres connection", zap.Error(err))
	}

	grm, err := postgres.NewGormFromPostgresConnection(pg.Db)
	if err != nil {
		l.Fatal("Failed to create gorm instance", zap.Error(err))
	}

	migrator := migrations.NewMigrator(pg.Db, grm, l, cfg)
	if err = migrator.MigrateAll(); err != nil {
		l.Fatal("Failed to migrate", zap.Error(err))
	}

	contractStore := postgresContractStore.NewPostgresContractStore(grm, l, cfg)
	if err := contractStore.InitializeCoreContracts(); err != nil {
		log.Fatalf("Failed to initialize core contracts: %v", err)
	}

	cm := contractManager.NewContractManager(contractStore, client, af, sdc, l)

	mds := pgStorage.NewPostgresBlockStore(grm, l, cfg)
	if err != nil {
		log.Fatalln(err)
	}

	sm := stateManager.NewEigenStateManager(l, grm)
	if err := eigenState.LoadEigenStateModels(sm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to load eigen state models", zap.Error(err))
	}

	msm := metaStateManager.NewMetaStateManager(grm, l, cfg)
	if err := metaState.LoadMetaStateModels(msm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to load meta state models", zap.Error(err))
	}

	fetchr := fetcher.NewFetcher(client, cfg, l)

	cc := sequentialContractCaller.NewSequentialContractCaller(client, cfg, cfg.EthereumRpcConfig.ContractCallBatchSize, l)

	idxr := indexer.NewIndexer(mds, contractStore, cm, client, fetchr, cc, grm, l, cfg)

	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)

	rc, err := rewards.NewRewardsCalculator(cfg, grm, mds, sog, sdc, l)
	if err != nil {
		l.Sugar().Fatalw("Failed to create rewards calculator", zap.Error(err))
	}

	rcq := rewardsCalculatorQueue.NewRewardsCalculatorQueue(rc, l)

	p := pipeline.NewPipeline(fetchr, idxr, mds, sm, msm, rc, rcq, cfg, sdc, eb, l)
	rps := proofs.NewRewardsProofsStore(rc, l)
	pds := protocolDataService.NewProtocolDataService(sm, grm, l, cfg)
	rds := rewardsDataService.NewRewardsDataService(grm, l, cfg, rc)

	// Initialize the transaction backfiller
	transactionBackfillerConfig := &transactionBackfiller.TransactionBackfillerConfig{
		Workers: cfg.BackfillConfig.Workers,
	}
	txBackfiller := transactionBackfiller.NewTransactionBackfiller(
		transactionBackfillerConfig,
		l,
		cfg,
		fetchr,
		mds,
	)

	// Initialize the backfiller service
	bfs := backfillerService.NewBackfillerService(
		txBackfiller,
		grm,
		l,
		cfg,
	)

	scc, err := sidecarClient.NewSidecarClient(cfg.SidecarPrimaryConfig.Url, !cfg.SidecarPrimaryConfig.Secure)
	if err != nil {
		l.Sugar().Fatalw("Failed to create sidecar client", zap.Error(err))
	}

	// Create new sidecar instance
	_ = sidecar.NewSidecar(&sidecar.SidecarConfig{
		GenesisBlockNumber: cfg.GetGenesisBlockNumber(),
	}, cfg, mds, p, sm, msm, rc, rcq, rps, l, client)

	rpc := rpcServer.NewRpcServer(&rpcServer.RpcServerConfig{
		GrpcPort: cfg.RpcConfig.GrpcPort,
		HttpPort: cfg.RpcConfig.HttpPort,
	}, mds, rc, rcq, eb, rps, pds, rds, bfs, scc, sdc, l, cfg)

	// RPC channel to notify the RPC server to shutdown gracefully
	rpcChannel := make(chan bool)
	if err := rpc.Start(ctx, rpcChannel); err != nil {
		l.Sugar().Fatalw("Failed to start RPC server", zap.Error(err))
	}

	block, err := fetchr.FetchBlock(ctx, 1215893)
	if err != nil {
		l.Sugar().Fatalw("Failed to fetch block", zap.Error(err))
	}

	transactionHash := "0xf6775c38af1d2802bcbc2b7c8959c0d5b48c63a14bfeda0261ba29d76c68c423"
	transaction := &ethereum.EthereumTransaction{}

	for _, tx := range block.Block.Transactions {
		if tx.Hash.Value() == transactionHash {
			transaction = tx
			break
		}
	}
	fmt.Printf("Transaction: %+v\n", transaction)
}
