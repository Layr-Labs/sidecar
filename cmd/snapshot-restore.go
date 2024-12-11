package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/spf13/cobra"
)

var snapshotRestoreFromPath string
var cleanRestore bool

var snapshotRestoreCmd = &cobra.Command{
	Use:   "snapshot-restore",
	Short: "Restore from a snapshot",
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize logger (in a real scenario, you'd likely pass in a config)
		l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: true}) // or false depending on your need

		// Retrieve database connection details from global flags
		dbHost, _ := cmd.Flags().GetString("database.host")
		dbName, _ := cmd.Flags().GetString("database.db_name")
		dbUser, _ := cmd.Flags().GetString("database.user")
		dbPassword, _ := cmd.Flags().GetString("database.password")
		dbPort, _ := cmd.Flags().GetInt("database.port")

		// Log database connection details at info level
		l.Sugar().Infow("Database connection details",
			"host", dbHost,
			"name", dbName,
			"user", dbUser,
			"port", dbPort,
		)

		// Log the snapshot path
		l.Sugar().Infow("Snapshot path", "path", snapshotRestoreFromPath)

		// Set the password in an environment variable for pg_restore
		if err := os.Setenv("PGPASSWORD", dbPassword); err != nil {
			l.Sugar().Fatalw("Failed to set environment variable", "error", err)
			return err
		}

		// Construct the pg_restore command with a context for timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		pgRestoreCmd := exec.CommandContext(ctx,
			"pg_restore",
			"--host", dbHost,
			"--port", fmt.Sprintf("%d", dbPort),
			"--username", dbUser,
			"--dbname", dbName,
			"--no-owner",
		)

		// Add the clean option if specified
		if cleanRestore {
			pgRestoreCmd.Args = append(pgRestoreCmd.Args, "--clean")
		}

		// Add the snapshot path
		pgRestoreCmd.Args = append(pgRestoreCmd.Args, snapshotRestoreFromPath)

		// Direct command output to stdout/stderr
		pgRestoreCmd.Stdout = os.Stdout
		pgRestoreCmd.Stderr = os.Stderr

		l.Sugar().Infow("Running pg_restore command",
			"command", pgRestoreCmd.String(),
		)

		// Run the pg_restore command
		if err := pgRestoreCmd.Run(); err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				l.Sugar().Errorw("pg_restore command timed out",
					"error", err,
					"command", pgRestoreCmd.String(),
				)
				return fmt.Errorf("pg_restore command timed out: %w", err)
			}
			l.Sugar().Errorw("Failed to run pg_restore",
				"error", err,
				"command", pgRestoreCmd.String(),
			)
			return fmt.Errorf("failed to run pg_restore: %w", err)
		}

		l.Sugar().Infow("Successfully restored from snapshot")
		return nil
	},
}

func init() {
	rootCmd.AddCommand(snapshotRestoreCmd)
	snapshotRestoreCmd.Flags().StringVarP(&snapshotRestoreFromPath, "snapshot-restore-from-path", "p", "", "Path to snapshot file (required)")
	snapshotRestoreCmd.Flags().BoolVar(&cleanRestore, "clean", false, "Clean restore (default false)")
	snapshotRestoreCmd.MarkFlagRequired("snapshot-restore-from-path")
}
