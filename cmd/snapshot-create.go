package cmd

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/spf13/cobra"
)

var (
	snapshotCreateToPath string
	snapshotFidelity     string
	s3Path               string
	compressSnapshot     bool
)

var snapshotCreateCmd = &cobra.Command{
	Use:   "snapshot-create",
	Short: "Create a snapshot of the database",
	Long: `Create a snapshot of the database.

Currently available fidelity levels:
- archive (default): includes chain data, EigenModel state, rewards, and staker-operator table data.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize logger (in a real scenario, you'd likely pass in a config)
		l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: true})

		// Retrieve database connection details from global flags
		dbHost, _ := cmd.Flags().GetString("database.host")
		dbName, _ := cmd.Flags().GetString("database.db_name")
		dbUser, _ := cmd.Flags().GetString("database.user")
		dbPassword, _ := cmd.Flags().GetString("database.password")
		dbPort, _ := cmd.Flags().GetInt("database.port")
		schemaName, _ := cmd.Flags().GetString("database.schema_name")

		// Retrieve chain and fidelity
		chain, _ := cmd.Flags().GetString("chain")

		// Validate fidelity
		if snapshotFidelity != "archive" {
			l.Sugar().Warnw("Unsupported fidelity specified; falling back to 'archive'.",
				"requested", snapshotFidelity,
				"used", "archive",
			)
			snapshotFidelity = "archive"
		}

		// Log database connection details at info level
		l.Sugar().Infow("Database connection details",
			"host", dbHost,
			"name", dbName,
			"user", dbUser,
			"port", dbPort,
			"schema", schemaName,
		)

		// If no specific file name is provided, generate one
		if snapshotCreateToPath == "" {
			fileNameSuffix := time.Now().UTC().Format("2006-01-02-15-04-05")
			snapshotCreateToPath = fmt.Sprintf("sidecar-snapshot-%s-%s-%s.dump", chain, snapshotFidelity, fileNameSuffix)
		}

		// Log the snapshot file path
		l.Sugar().Infow("Snapshot will be created at this file", "file", snapshotCreateToPath)

		// Set the password in an environment variable for pg_dump
		if err := os.Setenv("PGPASSWORD", dbPassword); err != nil {
			l.Sugar().Fatalw("Failed to set environment variable", "error", err)
			return err
		}

		// Construct the pg_dump command with a context for timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		pgDumpCmd := exec.CommandContext(ctx,
			"pg_dump",
			"--host", dbHost,
			"--port", fmt.Sprintf("%d", dbPort),
			"--username", dbUser,
			"--format", "custom",
			"--file", snapshotCreateToPath,
			"--schema", schemaName,
			dbName,
		)

		// Direct command output to stdout/stderr for visibility
		pgDumpCmd.Stdout = os.Stdout
		pgDumpCmd.Stderr = os.Stderr

		l.Sugar().Infow("Running pg_dump command",
			"command", pgDumpCmd.String(),
		)

		// Run the pg_dump command
		if err := pgDumpCmd.Run(); err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				l.Sugar().Errorw("pg_dump command timed out",
					"error", err,
					"command", pgDumpCmd.String(),
				)
				return fmt.Errorf("pg_dump command timed out: %w", err)
			}
			l.Sugar().Errorw("Failed to run pg_dump",
				"error", err,
				"command", pgDumpCmd.String(),
			)
			return fmt.Errorf("failed to run pg_dump: %w", err)
		}

		l.Sugar().Infow("Successfully created snapshot", "file", snapshotCreateToPath)

		// Compress the snapshot if the flag is set
		if compressSnapshot {
			compressedSnapshot := snapshotCreateToPath + ".tar.gz"
			l.Sugar().Infow("Compressing snapshot", "file", compressedSnapshot)
			tarCmd := exec.Command("tar", "-czf", compressedSnapshot, "-C", filepath.Dir(snapshotCreateToPath), filepath.Base(snapshotCreateToPath))
			tarCmd.Stdout = os.Stdout
			tarCmd.Stderr = os.Stderr

			if err := tarCmd.Run(); err != nil {
				l.Sugar().Errorw("Snapshot compression failed", "error", err)
				return fmt.Errorf("snapshot compression failed: %w", err)
			}

			l.Sugar().Infow("Successfully compressed snapshot", "file", compressedSnapshot)
			snapshotCreateToPath = compressedSnapshot
		}

		// Upload to S3 if s3Path is specified
		if s3Path != "" {
			l.Sugar().Infow("Uploading to S3", "path", s3Path)
			s3Cmd := exec.Command("aws", "s3", "cp", snapshotCreateToPath, s3Path)
			s3Cmd.Stdout = os.Stdout
			s3Cmd.Stderr = os.Stderr

			if err := s3Cmd.Run(); err != nil {
				l.Sugar().Errorw("Upload to S3 failed", "error", err)
				return fmt.Errorf("upload to S3 failed: %w", err)
			}

			l.Sugar().Infow("Successfully uploaded snapshot to S3", "file", snapshotCreateToPath)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(snapshotCreateCmd)
	snapshotCreateCmd.Flags().StringVarP(&snapshotCreateToPath, "snapshot-create-to-path", "f", "", "Path to save the snapshot file to (default is generated based on timestamp and chain)")
	snapshotCreateCmd.Flags().StringVar(&snapshotFidelity, "fidelity", "archive", "The fidelity level of the snapshot: 'archive' only currently supported.")
	snapshotCreateCmd.Flags().StringVar(&s3Path, "s3-path", "", "S3 path to upload the snapshot to (e.g., s3://bucket/folder/), Default doesn't upload. It will only upload the compressed snapshot if --compress is set")
	snapshotCreateCmd.Flags().BoolVar(&compressSnapshot, "compress", false, "Compress the snapshot and save as well as the original snapshot with .tar.gz extension")
}
