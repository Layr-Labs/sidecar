package cmd

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Layr-Labs/go-sidecar/internal/clients/ethereum"
	"github.com/Layr-Labs/go-sidecar/internal/clients/etherscan"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/contractCaller"
	"github.com/Layr-Labs/go-sidecar/internal/contractManager"
	"github.com/Layr-Labs/go-sidecar/internal/contractStore/sqliteContractStore"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/avsOperators"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/operatorShares"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/rewardSubmissions"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stakerDelegations"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stakerShares"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/submittedDistributionRoots"
	"github.com/Layr-Labs/go-sidecar/internal/fetcher"
	"github.com/Layr-Labs/go-sidecar/internal/indexer"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/metrics"
	"github.com/Layr-Labs/go-sidecar/internal/pipeline"
	"github.com/Layr-Labs/go-sidecar/internal/shutdown"
	"github.com/Layr-Labs/go-sidecar/internal/sidecar"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	sqliteBlockStore "github.com/Layr-Labs/go-sidecar/internal/storage/sqlite"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run the sidecar",
	Run: func(cmd *cobra.Command, args []string) {
		initRunCmd(cmd)
		cfg := config.NewConfig()
		ctx := context.Background()

		l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

		sdc, err := metrics.InitStatsdClient(cfg.StatsdUrl)
		if err != nil {
			l.Sugar().Fatal("Failed to setup statsd client", zap.Error(err))
		}

		etherscanClient := etherscan.NewEtherscanClient(cfg, l)
		client := ethereum.NewClient(cfg.EthereumRpcConfig.BaseUrl, l)

		if !cfg.SqliteConfig.InMemory {
			if err := sqlite.InitSqliteDir(cfg.GetSqlitePath()); err != nil {
				l.Error("Failed to initialize sqlite directory", zap.Error(err))
				panic(err)
			}
		}

		db := sqlite.NewSqlite(&sqlite.SqliteConfig{
			Path:           cfg.GetSqlitePath(),
			ExtensionsPath: cfg.SqliteConfig.ExtensionsPath,
		}, l)

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

		if _, err := avsOperators.NewAvsOperatorsModel(sm, grm, l, cfg); err != nil {
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
		if _, err := submittedDistributionRoots.NewSubmittedDistributionRootsModel(sm, grm, l, cfg); err != nil {
			l.Sugar().Fatalw("Failed to create SubmittedDistributionRootsModel", zap.Error(err))
		}
		if _, err := rewardSubmissions.NewRewardSubmissionsModel(sm, grm, l, cfg); err != nil {
			l.Sugar().Fatalw("Failed to create RewardSubmissionsModel", zap.Error(err))
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

		// Start the sidecar main process in a goroutine so that we can listen for a shutdown signal
		go sidecar.Start(ctx)

		l.Sugar().Info("Started Sidecar")

		gracefulShutdown := shutdown.CreateGracefulShutdownChannel()

		done := make(chan bool)
		shutdown.ListenForShutdown(gracefulShutdown, done, func() {
			l.Sugar().Info("Shutting down...")
			rpcChannel <- true
			sidecar.ShutdownChan <- true
		}, time.Second*5, l)
	},
}

func initRunCmd(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if err := viper.BindPFlag(config.KebabToSnakeCase(f.Name), f); err != nil {
			fmt.Printf("Failed to bind flag '%s' - %+v\n", f.Name, err)
		}
		if err := viper.BindEnv(f.Name); err != nil {
			fmt.Printf("Failed to bind env '%s' - %+v\n", f.Name, err)
		}

	})
}
