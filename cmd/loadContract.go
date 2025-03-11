package cmd

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/postgres"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres/migrations"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var loadContractCmd = &cobra.Command{
	Use:   "load-contract",
	Short: "Load a contract",
	Long:  "Load a contract",
	RunE: func(cmd *cobra.Command, args []string) error {
		initLoadContractCmd(cmd)
		cfg := config.NewConfig()

		ctx := context.Background()

		l, err := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

		metricsClients, err := metrics.InitMetricsSinksFromConfig(cfg, l)
		if err != nil {
			return fmt.Errorf("failed to setup metrics sink: %w", err)
		}

		sink, err := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, metricsClients)
		if err != nil {
			return fmt.Errorf("failed to setup metrics sink: %w", err)
		}

		client := ethereum.NewClient(ethereum.ConvertGlobalConfigToEthereumConfig(&cfg.EthereumRpcConfig), l)

		af := abiFetcher.NewAbiFetcher(client, &http.Client{Timeout: 5 * time.Second}, l, cfg, []abiSource.AbiSource{})

		pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)

		pg, err := postgres.NewPostgres(pgConfig)
		if err != nil {
			return fmt.Errorf("failed to setup postgres connection: %w", err)
		}

		grm, err := postgres.NewGormFromPostgresConnection(pg.Db)
		if err != nil {
			return fmt.Errorf("failed to create gorm instance: %w", err)
		}

		migrator := migrations.NewMigrator(pg.Db, grm, l, cfg)
		if err = migrator.MigrateAll(); err != nil {
			return fmt.Errorf("failed to migrate: %w", err)
		}

		contractStore := postgresContractStore.NewPostgresContractStore(grm, l, cfg)

		cm := contractManager.NewContractManager(contractStore, client, af, sink, l)

		if cfg.LoadContractConfig.FromFile != "" {
			filename := cfg.LoadContractConfig.FromFile

			err = contractStore.InitializeExternalContracts(filename)
			if err != nil {
				return fmt.Errorf("failed to initialize external contracts: %w", err)
			}
		} else {
			blockNumber := cfg.LoadContractConfig.BlockNumber
			contractAddress := cfg.LoadContractConfig.ContractAddress
			contractAbi := cfg.LoadContractConfig.ContractAbi
			implementationForAddress := cfg.LoadContractConfig.ImplementationForAddress
			implementationAbi := cfg.LoadContractConfig.ImplementationAbi

			err = cm.InitializeLoadingContract(ctx, blockNumber, contractAddress, contractAbi, implementationForAddress, implementationAbi)
			if err != nil {
				return fmt.Errorf("failed to initialize loading a contract: %w", err)
			}
		}

		return nil
	},
}

func initLoadContractCmd(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if err := viper.BindPFlag(config.KebabToSnakeCase(f.Name), f); err != nil {
			fmt.Printf("Failed to bind flag '%s' - %+v\n", f.Name, err)
		}
		if err := viper.BindEnv(f.Name); err != nil {
			fmt.Printf("Failed to bind env '%s' - %+v\n", f.Name, err)
		}
	})
}
