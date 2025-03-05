package cmd

import (
	"fmt"

	"github.com/Layr-Labs/sidecar/pkg/contractStore/postgresContractStore"
	"github.com/Layr-Labs/sidecar/pkg/postgres"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres/migrations"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var batchLoadContractsCmd = &cobra.Command{
	Use:   "batch-load-contracts",
	Short: "Load a list of contracts in a batch",
	Long:  "Load a list of contracts in a batch",
	RunE: func(cmd *cobra.Command, args []string) error {
		initBatchLoadContractsCmd(cmd)
		cfg := config.NewConfig()

		l, err := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

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

		filename := cfg.BatchLoadContractsConfig.JsonFile

		err = contractStore.InitializeExternalContracts(filename)
		if err != nil {
			return fmt.Errorf("failed to initialize external contracts: %w", err)
		}

		return nil
	},
}

func initBatchLoadContractsCmd(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if err := viper.BindPFlag(config.KebabToSnakeCase(f.Name), f); err != nil {
			fmt.Printf("Failed to bind flag '%s' - %+v\n", f.Name, err)
		}
		if err := viper.BindEnv(f.Name); err != nil {
			fmt.Printf("Failed to bind env '%s' - %+v\n", f.Name, err)
		}
	})
}
