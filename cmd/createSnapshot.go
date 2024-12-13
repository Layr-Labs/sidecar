package cmd

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/spf13/cobra"
)

var (
	output          string
	snapshotType    string
	compressionType string
)

var createSnapshotCmd = &cobra.Command{
	Use:   "create-snapshot",
	Short: "Create a snapshot of the database",
	Long: `Create a snapshot of the database.

Currently available Type levels:
- archive (default): includes chain data, EigenModel state, rewards, and staker-operator table data.
`,
	RunE: func(cmd *cobra.Command, args []string) error {
		// Initialize logger
		l, err := logger.NewLogger(&logger.LoggerConfig{Debug: true})
		if err != nil {
			// If logger can't be initialized, just print and exit
			fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
			os.Exit(1)
		}

		// Validate compression type
		if compressionType != "" && compressionType != "custom" {
			l.Sugar().Fatalw("Invalid compression type", "compressionType", compressionType)
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
		schemaName, err := cmd.Flags().GetString("database.schema_name")
		if err != nil {
			l.Sugar().Fatalw("Failed to get schema name", "error", err)
			os.Exit(1)
		}

		// Validate Type
		if snapshotType != "archive" {
			l.Sugar().Warnw("Unsupported Type specified; falling back to 'archive'.",
				"requested", snapshotType,
				"used", "archive",
			)
			snapshotType = "archive"
		}

		// Log database connection details without password
		l.Sugar().Infow("Database connection details",
			"host", dbHost,
			"name", dbName,
			"user", dbUser,
			"port", dbPort,
			"schema", schemaName,
		)

		// Prepare pg_dump command using connection string
		connectionString := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s", dbUser, dbPassword, dbHost, dbPort, dbName)
		dumpCmd := []string{
			"pg_dump",
			connectionString,
		}

		// If a schema is specified, tell pg_dump to limit dump to that schema
		if schemaName != "" {
			dumpCmd = append(dumpCmd, "-n", schemaName)
		}

		// If compression is requested, use the -Fc option
		if compressionType == "custom" {
			dumpCmd = append(dumpCmd, "-Fc")
		}

		if output != "" {
			// If output is specified, we consider output as a full file path
			dumpDir := filepath.Dir(output)

			// Ensure the dump directory exists
			if err := os.MkdirAll(dumpDir, 0755); err != nil {
				l.Sugar().Fatalw("Failed to create output directory", "directory", dumpDir, "error", err)
				os.Exit(1)
			}

			dumpCmd = append(dumpCmd, "-f", output)
		} else {
			// If no output file is specified, dump to stdout
			dumpCmd = append(dumpCmd, "-f", "/dev/stdout")
		}

		// Log starting snapshot without including the password
		safeConnectionString := fmt.Sprintf("postgresql://%s:****@%s:%d/%s?sslmode=disable", dbUser, dbHost, dbPort, dbName)
		l.Sugar().Infow("Starting database snapshot",
			"connection", safeConnectionString,
			"command", dumpCmd,
		)

		// Execute pg_dump command
		cmdExec := exec.Command(dumpCmd[0], dumpCmd[1:]...)
		outputBytes, err := cmdExec.CombinedOutput()
		if err != nil {
			l.Sugar().Fatalw("Failed to create database snapshot", "error", err, "output", string(outputBytes))
			os.Exit(1)
		}

		l.Sugar().Infow("Successfully created snapshot", "file", output)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(createSnapshotCmd)
	createSnapshotCmd.Flags().StringVarP(&output, "output", "f", "", "Path to save the snapshot file to (default is stdout if not specified)")
	createSnapshotCmd.Flags().StringVar(&snapshotType, "snapshot-type", "archive", "The type of the snapshot: 'archive' only currently supported.")
	createSnapshotCmd.Flags().StringVar(&compressionType, "compression-type", "", "Compression type for the snapshot: 'none' (default if empty), 'custom' for -Fc")
}
