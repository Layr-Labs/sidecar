package cmd

import (
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"strings"
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
	rootCmd.PersistentFlags().StringP("datadir", "d", "~/.sidecar", "The data directory")
	rootCmd.PersistentFlags().StringP("chain", "c", "mainnet", "The chain to use (mainnet, holesky, preprod")
	rootCmd.PersistentFlags().String("statsd.url", "", `e.g. "localhost:8125"`)

	rootCmd.PersistentFlags().String("ethereum.rpc-url", "", `e.g. "http://34.229.43.36:8545"`)
	rootCmd.PersistentFlags().String("ethereum.ws-url", "", `e.g. "ws://34.229.43.36:8546"`)

	rootCmd.PersistentFlags().String("etherscan.api-keys", "", `Comma-separated string of keys. e.g. "key1,key2,key3"`)

	rootCmd.PersistentFlags().Bool("sqlite.in-memory", false, `"true" or "false"`)
	rootCmd.PersistentFlags().String("sqlite.db-file-path", "", `e.g. "/tmp/sidecar.db"`)
	rootCmd.PersistentFlags().StringArray("sqlite.extensions-path", []string{}, `e.g. "./sqlite-extensions"`)

	rootCmd.PersistentFlags().Int("rpc.grpc-port", 7100, `e.g. 7100`)
	rootCmd.PersistentFlags().Int("rpc.http-port", 7101, `e.g. 7101`)

	rootCmd.PersistentFlags().VisitAll(func(f *pflag.Flag) {
		key := config.KebabToSnakeCase(f.Name)
		viper.BindPFlag(key, f) //nolint:errcheck
		viper.BindEnv(key)      //nolint:errcheck
	})

	rootCmd.AddCommand(runCmd)
}

func initConfig(cmd *cobra.Command) {
	viper.SetEnvPrefix("sidecar")

	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))

	viper.AutomaticEnv()
}
