package cmd

import (
	"context"
	"fmt"
	"os"

	"github.com/Layr-Labs/sidecar/pkg/abiFetcher"
	"github.com/Layr-Labs/sidecar/pkg/abiSource"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/postgres"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres/migrations"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var loadContractCmd = &cobra.Command{
	Use:   "load-contract [file]",
	Short: "Load a contract",
	Long:  `Load a contract from a file or stdin. If a file path is provided as the last argument, it will be used. If no file is provided, it will attempt to read from stdin.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		initLoadContractCmd(cmd)
		cfg := config.NewConfig()

		ctx := context.Background()

		l, err := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

		client := ethereum.NewClient(ethereum.ConvertGlobalConfigToEthereumConfig(&cfg.EthereumRpcConfig), l)

		af := abiFetcher.NewAbiFetcher(client, abiFetcher.DefaultHttpClient(), l, []abiSource.AbiSource{})

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

		cs := postgresContractStore.NewPostgresContractStore(grm, l, cfg)

		// Create the contract manager
		cm := contractManager.NewContractManager(grm, cs, client, af, l)

		var filename string
		var useFile bool
		var useStdin bool

		// Check if a file path is provided as a positional argument
		if len(args) > 0 {
			filename = args[0]
			useFile = true
		} else if cfg.LoadContractConfig.FromFile != "" {
			// Check if a file path is provided via the --from-file flag
			filename = cfg.LoadContractConfig.FromFile
			useFile = true
		} else {
			// Check if we should read from stdin (if stdin is not a terminal)
			stdinInfo, err := os.Stdin.Stat()
			if err == nil && (stdinInfo.Mode()&os.ModeCharDevice) == 0 {
				useStdin = true
			}
		}

		// Process based on input source
		if cfg.LoadContractConfig.Batch {
			if useFile {
				// Load contracts from file
				err := cs.InitializeExternalContracts(filename)
				if err != nil {
					return fmt.Errorf("failed to initialize external contracts from file: %w", err)
				}
				return nil
			}
			if useStdin {
				// Read directly from stdin
				err := cs.InitializeExternalContractsFromReader(os.Stdin)
				if err != nil {
					return fmt.Errorf("failed to initialize external contracts from stdin: %w", err)
				}
				return nil
			}
			// If batch mode is enabled but no input source is provided
			return fmt.Errorf("batch mode requires a file path or stdin input")
		} else {
			// If no file or stdin is provided, use the individual contract parameters
			params := contractManager.ContractLoadParams{
				Address:          cfg.LoadContractConfig.Address,
				Abi:              cfg.LoadContractConfig.Abi,
				BytecodeHash:     cfg.LoadContractConfig.BytecodeHash,
				BlockNumber:      cfg.LoadContractConfig.BlockNumber,
				AssociateToProxy: cfg.LoadContractConfig.AssociateToProxy,
			}

			_, err = cm.LoadContract(ctx, params)
			if err != nil {
				return fmt.Errorf("failed to load contract: %w", err)
			}

			return nil
		}
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
