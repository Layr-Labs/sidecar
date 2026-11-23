package cmd

import (
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/proofs"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	pgStorage "github.com/Layr-Labs/sidecar/pkg/storage/postgres"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var exportClaimsCmd = &cobra.Command{
	Use:   "export-claims",
	Short: "Write a claim proof for every earner in the latest claimable distribution root",
	Long: `Write claim-amounts.json, root.json and claims/<earner>.json for the latest active, non-disabled distribution root.
The files can be hosted statically so earners can claim without the sidecar API. Requires a database with gold tables (full or archive snapshot).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		initExportClaimsCmd(cmd)
		cfg := config.NewConfig()

		output, err := cmd.Flags().GetString("output")
		if err != nil || output == "" {
			return fmt.Errorf("--output is required")
		}

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

		pg, err := postgres.NewPostgres(postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig))
		if err != nil {
			return fmt.Errorf("failed to setup postgres connection: %w", err)
		}
		grm, err := postgres.NewGormFromPostgresConnection(pg.Db)
		if err != nil {
			return fmt.Errorf("failed to create gorm instance: %w", err)
		}

		rc, err := rewards.NewRewardsCalculator(cfg, grm, pgStorage.NewPostgresBlockStore(grm, l, cfg), stakerOperators.NewStakerOperatorGenerator(grm, l, cfg), sink, l)
		if err != nil {
			return fmt.Errorf("failed to create rewards calculator: %w", err)
		}

		root, err := rc.FindClaimableDistributionRoot(-1)
		if err != nil {
			return fmt.Errorf("failed to find claimable distribution root: %w", err)
		}
		if root == nil {
			return fmt.Errorf("no claimable distribution root found")
		}
		l.Sugar().Infow("Exporting claims", zap.Uint64("rootIndex", root.RootIndex), zap.String("root", root.Root), zap.String("snapshot", root.GetSnapshotDate()))

		rows, err := rc.FetchRewardsForSnapshot(root.GetSnapshotDate(), nil, nil)
		if err != nil {
			return fmt.Errorf("failed to fetch rewards for snapshot %s: %w", root.GetSnapshotDate(), err)
		}

		if err := proofs.WriteClaimFiles(output, cfg.Chain.String(), cfg.GetContractsMapForChain().RewardsCoordinator, root, rows); err != nil {
			return err
		}
		l.Sugar().Infow("Exported claims", zap.String("output", output), zap.Int("rows", len(rows)))
		return nil
	},
}

func initExportClaimsCmd(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if err := viper.BindPFlag(config.KebabToSnakeCase(f.Name), f); err != nil {
			fmt.Printf("Failed to bind flag '%s' - %+v\n", f.Name, err)
		}
		if err := viper.BindEnv(f.Name); err != nil {
			fmt.Printf("Failed to bind env '%s' - %+v\n", f.Name, err)
		}
	})
}
