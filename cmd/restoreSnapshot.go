package cmd

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"github.com/Layr-Labs/sidecar/internal/version"
	"go.uber.org/zap"

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

Note: This command restores --database.schema_name only if it's present in InputFile snapshot.
Follow the snapshot docs if you need to convert the snapshot to a different schema name than was used during snapshot creation.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		initRestoreSnapshotCmd(cmd)
		cfg := config.NewConfig()

		l, err := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})
		if err != nil {
			return fmt.Errorf("failed to initialize logger: %w", err)
		}

		metricsClients, err := metrics.InitMetricsSinksFromConfig(cfg, l)
		if err != nil {
			l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
		}

		sink, err := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, metricsClients)
		if err != nil {
			l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
		}

		ss := snapshot.NewSnapshotService(l, sink)

		err = ss.RestoreFromSnapshot(&snapshot.RestoreSnapshotConfig{
			SnapshotConfig: snapshot.SnapshotConfig{
				Chain:          cfg.Chain,
				SidecarVersion: version.GetVersion(),
				DBConfig:       snapshot.CreateSnapshotDbConfigFromConfig(cfg.DatabaseConfig),
				Verbose:        cfg.Debug,
			},
			Input:                   cfg.RestoreSnapshotConfig.InputFile,
			VerifySnapshotHash:      cfg.RestoreSnapshotConfig.VerifyHash,
			VerifySnapshotSignature: cfg.RestoreSnapshotConfig.VerifySignature,
			ManifestUrl:             cfg.RestoreSnapshotConfig.ManifestUrl,
		})
		sink.Flush()

		if err != nil {
			l.Sugar().Fatalw("Failed to restore snapshot", zap.Error(err))
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
