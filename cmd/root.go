package cmd

import (
	"os"
	"strings"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var rootCmd = &cobra.Command{
	Use:   "sidecar",
	Short: "The EigenLayer Sidecar makes it easy to interact with the EigenLayer protocol data",
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	initConfig(rootCmd)

	rootCmd.PersistentFlags().Bool("debug", false, `"true" or "false"`)
	rootCmd.PersistentFlags().StringP("chain", "c", "mainnet", "The chain to use (mainnet, holesky, preprod")

	rootCmd.PersistentFlags().String("ethereum.rpc-url", "", `e.g. "http://<hostname>:8545"`)
	rootCmd.PersistentFlags().Int(config.EthereumRpcContractCallBatchSize, 25, `The number of contract calls to batch together when fetching data from the Ethereum node`)
	rootCmd.PersistentFlags().Bool(config.EthereumRpcUseNativeBatchCall, true, `Use the native eth_call method for batch calls`)
	rootCmd.PersistentFlags().Int(config.EthereumRpcNativeBatchCallSize, 500, `The number of calls to batch together when using the native eth_call method`)
	rootCmd.PersistentFlags().Int(config.EthereumRpcChunkedBatchCallSize, 10, `The number of calls to make in parallel when using the chunked batch call method`)

	rootCmd.PersistentFlags().String(config.DatabaseHost, "localhost", `PostgreSQL host`)
	rootCmd.PersistentFlags().Int(config.DatabasePort, 5432, `PostgreSQL port`)
	rootCmd.PersistentFlags().String(config.DatabaseUser, "sidecar", `PostgreSQL username`)
	rootCmd.PersistentFlags().String(config.DatabasePassword, "", `PostgreSQL password`)
	rootCmd.PersistentFlags().String(config.DatabaseDbName, "sidecar", `PostgreSQL database name`)
	rootCmd.PersistentFlags().String(config.DatabaseSchemaName, "", `PostgreSQL schema name (default "public")`)

	rootCmd.PersistentFlags().Bool(config.RewardsValidateRewardsRoot, true, `Validate rewards roots while indexing`)
	rootCmd.PersistentFlags().Bool(config.RewardsGenerateStakerOperatorsTable, false, `Generate staker operators table while indexing`)

	rootCmd.PersistentFlags().Int("rpc.grpc-port", 7100, `gRPC port`)
	rootCmd.PersistentFlags().Int("rpc.http-port", 7101, `http rpc port`)

	rootCmd.PersistentFlags().Bool("datadog.statsd.enabled", false, `e.g. "true" or "false"`)
	rootCmd.PersistentFlags().String("datadog.statsd.url", "", `e.g. "localhost:8125"`)

	rootCmd.PersistentFlags().Bool("prometheus.enabled", false, `e.g. "true" or "false"`)
	rootCmd.PersistentFlags().Int("prometheus.port", 2112, `The port to run the prometheus server on`)

	// setup sub commands
	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(runOperatorRestakedStrategiesCmd)
	rootCmd.AddCommand(runVersionCmd)
	rootCmd.AddCommand(runDatabaseCmd)
	rootCmd.AddCommand(createSnapshotCmd)
	rootCmd.AddCommand(restoreSnapshotCmd)
	rootCmd.AddCommand(rpcCmd)

	// bind any subcommand flags
	createSnapshotCmd.PersistentFlags().String(config.SnapshotOutputFile, "", "Path to save the snapshot file to (required)")
	createSnapshotCmd.PersistentFlags().String(config.SnapshotOutputHashFile, "", "Path to save the output hash file to")

	restoreSnapshotCmd.PersistentFlags().String(config.SnapshotInputFile, "", "Path to the snapshot file (can override the metadata_url latest snapshot with an input_file or input_url)")
	restoreSnapshotCmd.PersistentFlags().String(config.SnapshotInputURL, "", "URL to the snapshot file (can override the metadata_url latest snapshot with an input_file or input_url)")
	restoreSnapshotCmd.PersistentFlags().String(config.SnapshotInputHashFile, "", "Path to the hash file")
	restoreSnapshotCmd.PersistentFlags().String(config.SnapshotInputHashURL, "", "URL to the hash file (overrides the hash_file)")

	rpcCmd.PersistentFlags().String(config.SidecarPrimaryUrl, "", `RPC url of the "primary" Sidecar instance in an HA environment`)

	rootCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		key := config.KebabToSnakeCase(f.Name)
		viper.BindPFlag(key, f) //nolint:errcheck
		viper.BindEnv(key)      //nolint:errcheck
	})

}

func initConfig(cmd *cobra.Command) {
	viper.SetEnvPrefix(config.ENV_PREFIX)

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))

	viper.AutomaticEnv()
}
