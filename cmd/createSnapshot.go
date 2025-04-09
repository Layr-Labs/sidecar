package cmd

import (
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/version"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"go.uber.org/zap"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/logger"
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

		metricsClients, err := metrics.InitMetricsSinksFromConfig(cfg, l)
		if err != nil {
			l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
		}

		sink, err := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, metricsClients)
		if err != nil {
			l.Sugar().Fatal("Failed to setup metrics sink", zap.Error(err))
		}

		ss := snapshot.NewSnapshotService(l, sink)

		_, err = ss.CreateSnapshot(&snapshot.CreateSnapshotConfig{
			SnapshotConfig: snapshot.SnapshotConfig{
				Chain:          cfg.Chain,
				SidecarVersion: version.GetVersion(),
				DBConfig:       snapshot.CreateSnapshotDbConfigFromConfig(cfg.DatabaseConfig),
				Verbose:        cfg.Debug,
			},
			DestinationPath:      cfg.CreateSnapshotConfig.OutputFile,
			GenerateMetadataFile: cfg.CreateSnapshotConfig.GenerateMetadataFile,
			Kind:                 snapshot.Kind(cfg.CreateSnapshotConfig.Kind),
		})

		sink.Flush()

		if err != nil {
			l.Sugar().Fatalw("Failed to create snapshot", zap.Error(err))
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
