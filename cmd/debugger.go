package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/coreContracts"
	coreContractMigrations "github.com/Layr-Labs/sidecar/pkg/coreContracts/migrations"
	"log"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/version"
	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller/sequentialContractCaller"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/eigenState"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/precommitProcessors"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator"
	"github.com/Layr-Labs/sidecar/pkg/eventBus"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/indexer"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metaState"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/pipeline"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/postgres/migrations"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	pgStorage "github.com/Layr-Labs/sidecar/pkg/storage/postgres"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var runDebuggerCmd = &cobra.Command{
	Use:   "debugger",
	Short: "Debugger utility",
	Run: func(cmd *cobra.Command, args []string) {
		initDebuggerCmd(cmd)
		cfg := config.NewConfig()

		l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

		l.Sugar().Infow("sidecar run",
			zap.String("version", version.GetVersion()),
			zap.String("commit", version.GetCommit()),
		)

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

		af := abiFetcher.NewAbiFetcher(client, abiFetcher.DefaultHttpClient(), l, []abiSource.AbiSource{})

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

		contractStore := postgresContractStore.NewPostgresContractStore(grm, l)

		cm := contractManager.NewContractManager(grm, contractStore, client, af, l)
		
		ccm := coreContracts.NewCoreContractManager(grm, cfg, contractStore, l)
		if _, err := ccm.MigrateCoreContracts(coreContractMigrations.GetCoreContractMigrations()); err != nil {
			l.Fatal("Failed to migrate core contracts", zap.Error(err))
		}

		mds := pgStorage.NewPostgresBlockStore(grm, l, cfg)
		if err != nil {
			log.Fatalln(err)
		}

		smig, err := stateMigrator.NewStateMigrator(grm, cfg, l)
		if err != nil {
			l.Sugar().Fatalw("Failed to create state migrator", zap.Error(err))
		}

		sm := stateManager.NewEigenStateManager(smig, l, grm)
		if err := eigenState.LoadEigenStateModels(sm, grm, l, cfg); err != nil {
			l.Sugar().Fatalw("Failed to load eigen state models", zap.Error(err))
		}

		msm := metaStateManager.NewMetaStateManager(grm, l, cfg)
		if err := metaState.LoadMetaStateModels(msm, grm, l, cfg); err != nil {
			l.Sugar().Fatalw("Failed to load meta state models", zap.Error(err))
		}

		precommitProcessors.LoadPrecommitProcessors(sm, grm, l)

		fetchr := fetcher.NewFetcher(client, &fetcher.FetcherConfig{UseGetBlockReceipts: cfg.EthereumRpcConfig.UseGetBlockReceipts}, l)

		cc := sequentialContractCaller.NewSequentialContractCaller(client, cfg, cfg.EthereumRpcConfig.ContractCallBatchSize, l)

		idxr := indexer.NewIndexer(mds, contractStore, cm, client, fetchr, cc, grm, l, cfg)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)

		rc, err := rewards.NewRewardsCalculator(cfg, grm, mds, sog, sdc, l)
		if err != nil {
			l.Sugar().Fatalw("Failed to create rewards calculator", zap.Error(err))
		}

		rcq := rewardsCalculatorQueue.NewRewardsCalculatorQueue(rc, l)

		_ = pipeline.NewPipeline(fetchr, idxr, mds, contractStore, cm, sm, msm, rc, rcq, cfg, sdc, eb, l)

		block, err := fetchr.FetchBlock(context.Background(), 21432449)
		if err != nil {
			l.Sugar().Fatalw("Failed to fetch block", zap.Error(err))
		}

		transactionHash := "0x0426297e2aad2a08a42c908649e4888747654a36a2cbc6631946e51641cf11ce"

		parsedTx, err := idxr.ParseInterestingTransactionsAndLogs(context.Background(), block)
		if err != nil {
			l.Sugar().Fatalw("Failed to parse transactions", zap.Error(err))
		}

		for _, tx := range parsedTx {
			if tx.Transaction.Hash.Value() == transactionHash {
				for _, log := range tx.Logs {
					jsonBytes, _ := json.MarshalIndent(log, "", "  ")
					fmt.Printf("Log: %s\n", string(jsonBytes))
				}
			}
		}
	},
}

func initDebuggerCmd(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if err := viper.BindPFlag(config.KebabToSnakeCase(f.Name), f); err != nil {
			fmt.Printf("Failed to bind flag '%s' - %+v\n", f.Name, err)
		}
		if err := viper.BindEnv(f.Name); err != nil {
			fmt.Printf("Failed to bind env '%s' - %+v\n", f.Name, err)
		}

	})
}
