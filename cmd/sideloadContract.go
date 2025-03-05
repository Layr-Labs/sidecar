package cmd

import (
	"context"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/abiSource/etherscan"
	"github.com/Layr-Labs/sidecar/pkg/abiSource/ipfs"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	etherscanClient "github.com/Layr-Labs/sidecar/pkg/clients/etherscan"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"net/http"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres/migrations"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var sideloadContractCmd = &cobra.Command{
	Use:   "sideload-contract",
	Short: "Sideload a contract",
	Long:  "Sideload a contract",
	RunE: func(cmd *cobra.Command, args []string) error {
		initSideloadContractCmd(cmd)
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

		abiSource := []abiSource.AbiSource{}
		httpClient := &http.Client{Timeout: 5 * time.Second}
		if cfg.SideloadConfig.AbiSource == "ipfs" {
			ipfsSource := ipfs.NewIpfs(httpClient, l, cfg)
			abiSource = append(abiSource, ipfsSource)
		} else if cfg.SideloadConfig.AbiSource == "etherscan" {
			ec := etherscanClient.NewEtherscanClient(httpClient, l, cfg)
			etherscanSource := etherscan.NewEtherscan(ec, l)
			abiSource = append(abiSource, etherscanSource)
		} else {
			return fmt.Errorf("abi source %s not supported", cfg.SideloadConfig.AbiSource)
		}

		af := abiFetcher.NewAbiFetcher(client, &http.Client{Timeout: 5 * time.Second}, l, cfg, abiSource)

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

		blockNumber := cfg.SideloadConfig.BlockNumber
		contractAddress := cfg.SideloadConfig.ContractAddress
		proxyContractAddress := cfg.SideloadConfig.ProxyContractAddress

		err = cm.CreateContractWithAbi(ctx, blockNumber, contractAddress)
		if err != nil {
			return fmt.Errorf("failed to create the contractAddress in contracts: %w", err)
		}
		err = cm.CreateContractWithAbi(ctx, blockNumber, proxyContractAddress)
		if err != nil {
			return fmt.Errorf("failed to create the proxyContractAddress in contracts: %w", err)
		}

		_, err = contractStore.CreateProxyContract(blockNumber, contractAddress, proxyContractAddress)
		if err != nil {
			return fmt.Errorf("failed to create a proxy contract: %w", err)
		}

		return nil
	},
}

func initSideloadContractCmd(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if err := viper.BindPFlag(config.KebabToSnakeCase(f.Name), f); err != nil {
			fmt.Printf("Failed to bind flag '%s' - %+v\n", f.Name, err)
		}
		if err := viper.BindEnv(f.Name); err != nil {
			fmt.Printf("Failed to bind env '%s' - %+v\n", f.Name, err)
		}
	})
}
