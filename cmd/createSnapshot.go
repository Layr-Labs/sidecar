package cmd

import (
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/pkg/snapshot"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var createSnapshotCmd = &cobra.Command{
	Use:   "create-snapshot",
	Short: "Create a snapshot of the database",
	Long:  "Create a snapshot of the database.",
	RunE: func(cmd *cobra.Command, args []string) error {
		initCreateSnapshotCmd(cmd)
		cfg := config.NewConfig()

		l, err := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

		svc, err := snapshot.NewSnapshotService(&snapshot.SnapshotConfig{
			OutputFile: cfg.SnapshotConfig.OutputFile,
			Host:       cfg.DatabaseConfig.Host,
			Port:       cfg.DatabaseConfig.Port,
			User:       cfg.DatabaseConfig.User,
			Password:   cfg.DatabaseConfig.Password,
			DbName:     cfg.DatabaseConfig.DbName,
			SchemaName: cfg.DatabaseConfig.SchemaName,
		}, l)
		if err != nil {
			return err
		}

		if err := svc.CreateSnapshot(); err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}

		return nil
	},
}

func initCreateSnapshotCmd(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if err := viper.BindPFlag(config.KebabToSnakeCase(f.Name), f); err != nil {
			fmt.Printf("Failed to bind flag '%s' - %+v\n", f.Name, err)
		}
		if err := viper.BindEnv(f.Name); err != nil {
			fmt.Printf("Failed to bind env '%s' - %+v\n", f.Name, err)
		}
	})
}
