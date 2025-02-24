package cmd

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/version"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var runVersionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show the version of the sidecar",
	Run: func(cmd *cobra.Command, args []string) {
		initVersionCmd(cmd)

		v := version.GetVersion()
		commit := version.GetCommit()

		fmt.Printf("SidecarVersion: %s\nCommit: %s\n", v, commit)
	},
}

func initVersionCmd(cmd *cobra.Command) {
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		if err := viper.BindPFlag(config.KebabToSnakeCase(f.Name), f); err != nil {
			fmt.Printf("Failed to bind flag '%s' - %+v\n", f.Name, err)
		}
		if err := viper.BindEnv(f.Name); err != nil {
			fmt.Printf("Failed to bind env '%s' - %+v\n", f.Name, err)
		}

	})
}
