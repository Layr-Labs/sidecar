package cmd

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/spf13/cobra"
)

var restoreSnapshotFromPath string

var restoreSnapshotCmd = &cobra.Command{
	Use:   "restore-snapshot",
	Short: "Restore from a snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize logger
		l, err := logger.NewLogger(&logger.LoggerConfig{Debug: true})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
			os.Exit(1)
		}

		// Retrieve database connection details from global flags
		dbHost, err := cmd.Flags().GetString("database.host")
		if err != nil {
			l.Sugar().Fatalw("Failed to get database host", "error", err)
			os.Exit(1)
		}
		dbName, err := cmd.Flags().GetString("database.db_name")
		if err != nil {
			l.Sugar().Fatalw("Failed to get database name", "error", err)
			os.Exit(1)
		}
		dbUser, err := cmd.Flags().GetString("database.user")
		if err != nil {
			l.Sugar().Fatalw("Failed to get database user", "error", err)
			os.Exit(1)
		}
		dbPassword, err := cmd.Flags().GetString("database.password")
		if err != nil {
			l.Sugar().Fatalw("Failed to get database password", "error", err)
			os.Exit(1)
		}
		dbPort, err := cmd.Flags().GetInt("database.port")
		if err != nil {
			l.Sugar().Fatalw("Failed to get database port", "error", err)
			os.Exit(1)
		}

		// Log the database connection details without password
		l.Sugar().Infow("Database connection details",
			"host", dbHost,
			"name", dbName,
			"user", dbUser,
			"port", dbPort,
		)

		// Log the snapshot path
		l.Sugar().Infow("Snapshot path", "path", restoreSnapshotFromPath)

		// Prepare pg_restore command using connection string
		connectionString := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
		restoreCmd := []string{
			"pg_restore",
			"--dbname", connectionString,
			"--no-owner",
		}

		if restoreSnapshotFromPath != "" {
			restoreCmd = append(restoreCmd, restoreSnapshotFromPath)
		} else {
			restoreCmd = append(restoreCmd, "/dev/stdin")
		}

		// Execute pg_restore command
		cmdExec := exec.Command(restoreCmd[0], restoreCmd[1:]...)
		outputBytes, err := cmdExec.CombinedOutput()
		if err != nil {
			l.Sugar().Errorw("Failed to restore from snapshot", "error", err, "output", string(outputBytes))
			return fmt.Errorf("restore snapshot failed: %v", err)
		}

		l.Sugar().Infow("Successfully restored from snapshot")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(restoreSnapshotCmd)
	restoreSnapshotCmd.Flags().StringVarP(&restoreSnapshotFromPath, "restore-snapshot-from-path", "p", "", "Path to snapshot file (use standard input if not specified)")
}
