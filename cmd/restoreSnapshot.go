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

var restoreSnapshotCmd = &cobra.Command{
	Use:   "restore-snapshot",
	Short: "Restore database from a snapshot file",
	Long: `Restore the database from a previously created snapshot file.

Note: It is recommended to use a fresh, clean database for restoration. Objects in the snapshot will overwrite existing objects in the database. 
This command restores --database.schema_name only if it's present in InputFile snapshot.
Follow the snapshot docs if you need to convert the snapshot to a different schema name than was used during snapshot creation.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		initRestoreSnapshotCmd(cmd)
		cfg := config.NewConfig()

		l, err := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

		svc, err := snapshot.NewSnapshotService(&snapshot.SnapshotConfig{
			InputFile:  cfg.SnapshotConfig.InputFile,
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

		if err := svc.RestoreSnapshot(); err != nil {
			return fmt.Errorf("failed to restore snapshot: %w", err)
		}

		return nil
	},
}

func initRestoreSnapshotCmd(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if err := viper.BindPFlag(config.KebabToSnakeCase(f.Name), f); err != nil {
			fmt.Printf("Failed to bind flag '%s' - %+v\n", f.Name, err)
		}
		if err := viper.BindEnv(f.Name); err != nil {
			fmt.Printf("Failed to bind env '%s' - %+v\n", f.Name, err)
		}
	})
}
