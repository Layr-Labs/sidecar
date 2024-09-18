package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Layr-Labs/go-sidecar/internal/clients/ethereum"
	"github.com/Layr-Labs/go-sidecar/internal/clients/etherscan"
	"github.com/Layr-Labs/go-sidecar/internal/contractCaller"
	"github.com/Layr-Labs/go-sidecar/internal/contractManager"
	"github.com/Layr-Labs/go-sidecar/internal/contractStore/sqliteContractStore"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/avsOperators"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/operatorShares"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stakerDelegations"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stakerShares"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/fetcher"
	"github.com/Layr-Labs/go-sidecar/internal/indexer"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/metrics"
	"github.com/Layr-Labs/go-sidecar/internal/pipeline"
	"github.com/Layr-Labs/go-sidecar/internal/sidecar"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	sqliteBlockStore "github.com/Layr-Labs/go-sidecar/internal/storage/sqlite"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"go.uber.org/zap"
)

func main() {
	ctx := context.Background()
	cfg := tests.GetConfig()

	fmt.Printf("Config: %+v\n", cfg)

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	sdc, err := metrics.InitStatsdClient(cfg.StatsdUrl)
	if err != nil {
		l.Sugar().Fatal("Failed to setup statsd client", zap.Error(err))
	}

	etherscanClient := etherscan.NewEtherscanClient(cfg, l)
	client := ethereum.NewClient(cfg.EthereumRpcConfig.BaseUrl, l)

	db := sqlite.NewSqlite(cfg.SqliteConfig.GetSqlitePath())

	grm, err := sqlite.NewGormSqliteFromSqlite(db)
	if err != nil {
		l.Error("Failed to create gorm instance", zap.Error(err))
		panic(err)
	}

	migrator := migrations.NewSqliteMigrator(grm, l)
	if err = migrator.MigrateAll(); err != nil {
		log.Fatalf("Failed to migrate: %v", err)
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

	if _, err := avsOperators.NewAvsOperators(sm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create AvsOperatorsModel", zap.Error(err))
	}
	if _, err := operatorShares.NewOperatorSharesModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create OperatorSharesModel", zap.Error(err))
	}
	if _, err := stakerDelegations.NewStakerDelegationsModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create StakerDelegationsModel", zap.Error(err))
	}
	if _, err := stakerShares.NewStakerSharesModel(sm, grm, l, cfg); err != nil {
		l.Sugar().Fatalw("Failed to create StakerSharesModel", zap.Error(err))
	}

	fetchr := fetcher.NewFetcher(client, cfg, l)

	cc := contractCaller.NewContractCaller(client, l)

	idxr := indexer.NewIndexer(mds, contractStore, etherscanClient, cm, client, fetchr, cc, l, cfg)

	p := pipeline.NewPipeline(fetchr, idxr, mds, sm, l)

	// Create new sidecar instance
	sidecar := sidecar.NewSidecar(&sidecar.SidecarConfig{
		GenesisBlockNumber: cfg.GetGenesisBlockNumber(),
	}, cfg, mds, p, sm, l, client)

	// RPC channel to notify the RPC server to shutdown gracefully
	rpcChannel := make(chan bool)
	err = sidecar.WithRpcServer(ctx, mds, sm, rpcChannel)
	if err != nil {
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

	logIndex := 4
	receipt := block.TxReceipts[transaction.Hash.Value()]
	var interestingLog *ethereum.EthereumEventLog

	for _, log := range receipt.Logs {
		if log.LogIndex.Value() == uint64(logIndex) {
			fmt.Printf("Log: %+v\n", log)
			interestingLog = log
		}
	}

	decodedLog, err := idxr.DecodeLogWithAbi(nil, receipt, interestingLog)
	if err != nil {
		l.Sugar().Fatalw("Failed to decode log", zap.Error(err))
	}
	l.Sugar().Infof("Decoded log: %+v", decodedLog)
}
