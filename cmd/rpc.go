package cmd

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/version"
	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/abiSource/etherscan"
	"github.com/Layr-Labs/sidecar/pkg/abiSource/ipfs"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	etherscanClient "github.com/Layr-Labs/sidecar/pkg/clients/etherscan"
	sidecarClient "github.com/Layr-Labs/sidecar/pkg/clients/sidecar"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller/sequentialStrategyCaller"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/eigenState"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/precommitProcessors"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator"
	"github.com/Layr-Labs/sidecar/pkg/eventBus"
	"github.com/Layr-Labs/sidecar/pkg/metrics/prometheus"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/proofs"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/Layr-Labs/sidecar/pkg/rewardsCalculatorQueue"
	"github.com/Layr-Labs/sidecar/pkg/rpcServer"
	"github.com/Layr-Labs/sidecar/pkg/service/aprDataService"
	"github.com/Layr-Labs/sidecar/pkg/service/protocolDataService"
	"github.com/Layr-Labs/sidecar/pkg/service/rewardsDataService"
	"github.com/Layr-Labs/sidecar/pkg/service/slashingDataService"
	"github.com/Layr-Labs/sidecar/pkg/shutdown"
	pgStorage "github.com/Layr-Labs/sidecar/pkg/storage/postgres"

	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var rpcCmd = &cobra.Command{
	Use:   "rpc",
	Short: "Run just the Sidecar RPC server",
	Run: func(cmd *cobra.Command, args []string) {
		initRpcCmd(cmd)
		cfg := config.NewConfig()
		cfg.SidecarPrimaryConfig.IsPrimary = false

		if len(cfg.SidecarPrimaryConfig.Url) == 0 {
			log.Fatalf("%s URL is required", config.SidecarPrimaryUrl)
		}

		ctx := context.Background()

		l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

		l.Sugar().Infow("sidecar rpc",
			zap.String("version", version.GetVersion()),
			zap.String("commit", version.GetCommit()),
			zap.String("chain", cfg.Chain.String()),
		)

		eb := eventBus.NewEventBus(l)

		metricsClients, err := metrics.InitMetricsSinksFromConfig(cfg, l)
		if err != nil {
			l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
		}

		sink, err := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, metricsClients)
		if err != nil {
			l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
		}

		client := ethereum.NewClient(ethereum.ConvertGlobalConfigToEthereumConfig(&cfg.EthereumRpcConfig), l)

		ipfs := ipfs.NewIpfs(ipfs.DefaultHttpClient(), l, cfg)
		etherscanClient := etherscanClient.NewEtherscanClient(etherscanClient.DefaultHttpClient(), l, cfg)
		etherscan := etherscan.NewEtherscan(etherscanClient, l)

		af := abiFetcher.NewAbiFetcher(client, abiFetcher.DefaultHttpClient(), l, []abiSource.AbiSource{ipfs, etherscan})

		pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

		pg, err := postgres.NewPostgres(pgConfig)
		if err != nil {
			l.Fatal("Failed to setup postgres connection", zap.Error(err))
		}

		grm, err := postgres.NewGormFromPostgresConnection(pg.Db)
		if err != nil {
			l.Fatal("Failed to create gorm instance", zap.Error(err))
		}

		cs := postgresContractStore.NewPostgresContractStore(grm, l)

		cm := contractManager.NewContractManager(grm, cs, client, af, l)

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

		precommitProcessors.LoadPrecommitProcessors(sm, grm, l)

		sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)

		rc, err := rewards.NewRewardsCalculator(cfg, grm, mds, sog, sink, l)
		if err != nil {
			l.Sugar().Fatalw("Failed to create rewards calculator", zap.Error(err))
		}

		rcq := rewardsCalculatorQueue.NewRewardsCalculatorQueue(rc, l)

		rps := proofs.NewRewardsProofsStore(rc, sink, l)

		pds := protocolDataService.NewProtocolDataService(sm, grm, l, cfg)
		rds := rewardsDataService.NewRewardsDataService(grm, l, cfg, rc)
		sds := slashingDataService.NewSlashingDataService(grm, l, cfg)

		sc := sequentialStrategyCaller.NewSequentialStrategyCaller(client, l)
		ads := aprDataService.NewAprDataService(grm, l, cfg, sc)

		go rcq.Process()

		scc, err := sidecarClient.NewSidecarClient(cfg.SidecarPrimaryConfig.Url, !cfg.SidecarPrimaryConfig.Secure)
		if err != nil {
			l.Sugar().Fatalw("Failed to create sidecar client", zap.Error(err))
		}

		rpc := rpcServer.NewRpcServer(&rpcServer.RpcServerConfig{
			GrpcPort: cfg.RpcConfig.GrpcPort,
			HttpPort: cfg.RpcConfig.HttpPort,
		}, mds, cs, cm, rc, rcq, eb, rps, pds, rds, sds, ads, scc, sink, l, cfg)

		// RPC channel to notify the RPC server to shutdown gracefully
		rpcChannel := make(chan bool)
		if err := rpc.Start(ctx, rpcChannel); err != nil {
			l.Sugar().Fatalw("Failed to start RPC server", zap.Error(err))
		}

		promChan := make(chan bool)
		if cfg.PrometheusConfig.Enabled {
			pServer := prometheus.NewPrometheusServer(&prometheus.PrometheusServerConfig{
				Port: cfg.PrometheusConfig.Port,
			}, l)
			if err := pServer.Start(promChan); err != nil {
				l.Sugar().Fatalw("Failed to start prometheus server", zap.Error(err))
			}
		}

		l.Sugar().Info("Started Sidecar")

		gracefulShutdown := shutdown.CreateGracefulShutdownChannel()

		done := make(chan bool)
		shutdown.ListenForShutdown(gracefulShutdown, done, func() {
			l.Sugar().Info("Shutting down...")
			rpcChannel <- true
		}, time.Second*5, l)
	},
}

func initRpcCmd(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if err := viper.BindPFlag(config.KebabToSnakeCase(f.Name), f); err != nil {
			fmt.Printf("Failed to bind flag '%s' - %+v\n", f.Name, err)
		}
		if err := viper.BindEnv(f.Name); err != nil {
			fmt.Printf("Failed to bind env '%s' - %+v\n", f.Name, err)
		}

	})
}
