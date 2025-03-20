package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts"
	coreContractMigrations "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	pgStorage "github.com/Layr-Labs/sidecar/pkg/storage/postgres"
	"github.com/Layr-Labs/sidecar/pkg/transactionBackfiller"
	"github.com/Layr-Labs/sidecar/pkg/transactionLogParser"
	"go.uber.org/zap"
)

func setup(ethConfig *ethereum.EthereumClientConfig) (
	*config.Config,
	*zap.Logger,
	*fetcher.Fetcher,
	storage.BlockStore,
	*contractManager.ContractManager,
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

	_, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		log.Fatal(err)
	}

	contractStore := postgresContractStore.NewPostgresContractStore(grm, l, cfg)

	ccm := coreContracts.NewCoreContractManager(grm, cfg, contractStore, l)
	if _, err := ccm.MigrateCoreContracts(coreContractMigrations.GetCoreContractMigrations()); err != nil {
		l.Fatal("Failed to migrate core contracts", zap.Error(err))
	}

	mds := pgStorage.NewPostgresBlockStore(grm, l, cfg)

	cm := contractManager.NewContractManager(grm, contractStore, client, af, l)

	fetchr := fetcher.NewFetcher(client, &fetcher.FetcherConfig{UseGetBlockReceipts: cfg.EthereumRpcConfig.UseGetBlockReceipts}, l)

	return cfg, l, fetchr, mds, cm, nil

}

type BackfillerPlugin struct {
	logParser  *transactionLogParser.TransactionLogParser
	blockStore storage.BlockStore
	logger     *zap.Logger
}

func NewBackfillerPlugin(
	bs storage.BlockStore,
	l *zap.Logger,
	cm *contractManager.ContractManager,
) *BackfillerPlugin {
	b := &BackfillerPlugin{
		blockStore: bs,
		logger:     l,
	}

	// IsInterestingAddress satisfies the IsInterestingAddress interface
	logParser := transactionLogParser.NewTransactionLogParser(l, cm, b)

	b.logParser = logParser
	return b
}

func (bp *BackfillerPlugin) IsInterestingAddress(address string) bool {
	// Custom logic to determine if an address is interesting
	return true
}

func (bp *BackfillerPlugin) DecodeAndStoreLog(
	block *ethereum.EthereumBlock,
	receipt *ethereum.EthereumTransactionReceipt,
	log *ethereum.EthereumEventLog,
) error {
	decodedLog, err := bp.logParser.DecodeLogWithAbi(nil, receipt, log)
	if err != nil {
		return err
	}
	fmt.Printf("Decoded log: %+v\n", decodedLog)

	insertedLog, err := bp.blockStore.InsertTransactionLog(
		receipt.TransactionHash.Value(),
		receipt.TransactionIndex.Value(),
		receipt.BlockNumber.Value(),
		decodedLog,
		decodedLog.OutputData,
		true,
	)
	bp.logger.Sugar().Debugw("Inserted log", zap.Any("log", insertedLog))
	return err
}

func main() {
	_, l, fetchr, mds, cm, err := setup(&ethereum.EthereumClientConfig{})

	if err != nil {
		log.Fatalln(err)
	}

	plugin := NewBackfillerPlugin(mds, l, cm)

	bf := transactionBackfiller.NewTransactionBackfiller(&transactionBackfiller.TransactionBackfillerConfig{}, l, fetchr, mds)

	startBlock := uint64(22020900)
	endBlock := uint64(22020901)

	message := &transactionBackfiller.BackfillerMessage{
		StartBlock: startBlock,
		EndBlock:   endBlock,
		IsInterestingLog: func(log *ethereum.EthereumEventLog) bool {
			return plugin.IsInterestingAddress(strings.ToLower(log.Address.Value()))
		},
		TransactionLogHandler: func(block *ethereum.EthereumBlock, receipt *ethereum.EthereumTransactionReceipt, log *ethereum.EthereumEventLog) error {
			return plugin.DecodeAndStoreLog(block, receipt, log)
		},
	}

	// ------------------------------------------------------------------------
	// Run a single time without using the queue
	// ------------------------------------------------------------------------
	bf.ProcessBlocks(context.Background(), message)

	// ------------------------------------------------------------------------
	// Use the queue
	// ------------------------------------------------------------------------
	go bf.Process()

	res, err := bf.EnqueueAndWait(context.Background(), message)
	fmt.Printf("Error: %v\n", err)
	fmt.Printf("Response: %v\n", res)
}
