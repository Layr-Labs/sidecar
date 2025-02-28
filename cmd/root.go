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
	rootCmd.PersistentFlags().String(config.DatabaseSSLMode, "disable", `PostgreSQL SSL mode (default "disable", could be one of: disable, require, verify-ca, verify-full)`)
	rootCmd.PersistentFlags().String(config.DatabaseSSLCert, "", `Path to client certificate file`)
	rootCmd.PersistentFlags().String(config.DatabaseSSLKey, "", `Path to client private key file`)
	rootCmd.PersistentFlags().String(config.DatabaseSSLRootCert, "", `Path to root certificate file`)

	rootCmd.PersistentFlags().Bool(config.RewardsValidateRewardsRoot, true, `Validate rewards roots while indexing`)
	rootCmd.PersistentFlags().Bool(config.RewardsGenerateStakerOperatorsTable, false, `Generate staker operators table while indexing`)

	rootCmd.PersistentFlags().Int("rpc.grpc-port", 7100, `gRPC port`)
	rootCmd.PersistentFlags().Int("rpc.http-port", 7101, `http rpc port`)

	rootCmd.PersistentFlags().Bool("datadog.statsd.enabled", false, `e.g. "true" or "false"`)
	rootCmd.PersistentFlags().String("datadog.statsd.url", "", `e.g. "localhost:8125"`)
	rootCmd.PersistentFlags().Float64(config.DataDogStatsdSampleRate, 1.0, `The sample rate to use for statsd metrics`)

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
	createSnapshotCmd.PersistentFlags().String(config.SnapshotOutputFile, "", "(deprecated, use --output) Path to save the snapshot file")
	createSnapshotCmd.PersistentFlags().String(config.SnapshotOutput, "", "Path to save the snapshot file")
	createSnapshotCmd.PersistentFlags().Bool(config.SnapshotOutputMetadataFile, true, "Generate a metadata file for the snapshot")
	createSnapshotCmd.PersistentFlags().String(config.SnapshotKind, "full", "The kind of snapshot to create (slim, full, or archive)")

	restoreSnapshotCmd.PersistentFlags().String(config.SnapshotInputFile, "", "(deprecated, use --input) Path to the snapshot file")
	restoreSnapshotCmd.PersistentFlags().String(config.SnapshotInput, "", "Path to the snapshot file")
	restoreSnapshotCmd.PersistentFlags().String(config.SnapshotManifestUrl, "https://sidecar.eigenlayer.xyz/snapshots/manifest.json", "URL to the snapshot manifest file")
	restoreSnapshotCmd.PersistentFlags().Bool(config.SnapshotVerifyHash, true, "Verify the hash of the snapshot file")
	restoreSnapshotCmd.PersistentFlags().Bool(config.SnapshotVerifySignature, false, "Verify the signature of the snapshot file")
	restoreSnapshotCmd.PersistentFlags().String(config.SnapshotKind, "full", "The kind of snapshot to restore (slim, full, or archive)")

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
