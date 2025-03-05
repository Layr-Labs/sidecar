package snapshot

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"go.uber.org/zap"
)

// Command constants for PostgreSQL operations
const (
	// PgDump is the command name for PostgreSQL database dump
	PgDump    = "pg_dump"
	// PgRestore is the command name for PostgreSQL database restore
	PgRestore = "pg_restore"
)

// SnapshotDatabaseConfig contains the configuration for connecting to a PostgreSQL database.
type SnapshotDatabaseConfig struct {
	// Host is the database server hostname
	Host        string
	// Port is the database server port
	Port        int
	// DbName is the name of the database to connect to
	DbName      string
	// User is the username for database authentication
	User        string
	// Password is the password for database authentication
	Password    string
	// SchemaName is the database schema to use
	SchemaName  string
	// SSLMode specifies the SSL mode for the connection
	SSLMode     string
	// SSLKey is the path to the client SSL key file
	SSLKey      string
	// SSLCert is the path to the client SSL certificate file
	SSLCert     string
	// SSLRootCert is the path to the SSL root certificate file
	SSLRootCert string
}

// IsValid checks if the database configuration is valid.
// It returns true if valid, or false and an error if invalid.
func (sdc *SnapshotDatabaseConfig) IsValid() (bool, error) {
	if sdc.DbName == "" {
		return false, fmt.Errorf("database name is required")
	}
	return true, nil
}

// SnapshotConfig contains the common configuration for snapshot operations.
type SnapshotConfig struct {
	// Chain identifies the blockchain network
	Chain          config.Chain
	// SidecarVersion is the version of the sidecar software
	SidecarVersion string
	// DBConfig contains the database connection configuration
	DBConfig       SnapshotDatabaseConfig
	// Verbose enables detailed logging
	Verbose        bool
}

// IsValid checks if the snapshot configuration is valid.
// It returns true if valid, or false and an error if invalid.
func (sc *SnapshotConfig) IsValid() (bool, error) {
	if sc.Chain == "" {
		return false, fmt.Errorf("chain is required")
	}
	if valid, err := sc.DBConfig.IsValid(); !valid || err != nil {
		return false, fmt.Errorf("invalid database configuration: %w", err)
	}
	return true, nil
}

// CreateSnapshotConfig extends SnapshotConfig with parameters specific to creating snapshots.
type CreateSnapshotConfig struct {
	SnapshotConfig
	// DestinationPath is the directory where the snapshot will be saved
	DestinationPath      string
	// GenerateMetadataFile indicates whether to generate a metadata file for the snapshot
	GenerateMetadataFile bool
	// Kind specifies the type of snapshot to create (Slim, Full, Archive)
	Kind                 Kind
}

// IsValid checks if the create snapshot configuration is valid.
// It returns true if valid, or false and an error if invalid.
func (csc *CreateSnapshotConfig) IsValid() (bool, error) {
	if csc.DestinationPath == "" {
		return false, fmt.Errorf("destination path is required")
	}
	if csc.Kind == "" {
		return false, fmt.Errorf("kind is required")
	}
	if valid, err := csc.SnapshotConfig.IsValid(); !valid || err != nil {
		return false, err
	}
	return true, nil
}

// RestoreSnapshotConfig extends SnapshotConfig with parameters specific to restoring snapshots.
type RestoreSnapshotConfig struct {
	SnapshotConfig
	// VerifySnapshotHash indicates whether to verify the snapshot hash
	VerifySnapshotHash      bool
	// VerifySnapshotSignature indicates whether to verify the snapshot signature
	VerifySnapshotSignature bool
	// SnapshotPublicKey is the public key used to verify the snapshot signature
	SnapshotPublicKey       string
	// ManifestUrl is the URL of the snapshot manifest
	ManifestUrl             string
	// Input is the path or URL to the snapshot file
	Input                   string
	// Kind specifies the type of snapshot to restore (Slim, Full, Archive)
	Kind                    Kind
}

// IsValid checks if the restore snapshot configuration is valid.
// It returns true if valid, or false and an error if invalid.
func (rsc *RestoreSnapshotConfig) IsValid() (bool, error) {
	if valid, err := rsc.SnapshotConfig.IsValid(); !valid || err != nil {
		return false, err
	}
	return true, nil
}

// CreateSnapshotDbConfigFromConfig creates a SnapshotDatabaseConfig from a config.DatabaseConfig.
// It sets default values for missing fields.
func CreateSnapshotDbConfigFromConfig(cfg config.DatabaseConfig) SnapshotDatabaseConfig {
	host := cfg.Host
	if host == "" {
		host = "localhost"
	}

	port := cfg.Port
	if cfg.Port == 0 {
		port = 5432
	}

	schemaName := cfg.SchemaName
	if schemaName == "" {
		schemaName = "public"
	}

	return SnapshotDatabaseConfig{
		Host:        host,
		Port:        port,
		DbName:      cfg.DbName,
		User:        cfg.User,
		Password:    cfg.Password,
		SchemaName:  schemaName,
		SSLMode:     cfg.SSLMode,
		SSLKey:      cfg.SSLKey,
		SSLCert:     cfg.SSLCert,
		SSLRootCert: cfg.SSLRootCert,
	}
}

// SnapshotService provides methods for creating and restoring database snapshots.
type SnapshotService struct {
	logger      *zap.Logger
	metricsSink *metrics.MetricsSink
}

// NewSnapshotService creates a new SnapshotService with the provided logger and metrics sink.
func NewSnapshotService(l *zap.Logger, ms *metrics.MetricsSink) *SnapshotService {
	return &SnapshotService{
		logger:      l,
		metricsSink: ms,
	}
}

// buildCommand constructs a command with the appropriate flags based on the configuration.
// It combines database connection flags with operation-specific flags.
func (ss *SnapshotService) buildCommand(flags []string, cfg SnapshotConfig) []string {
	cmd := append(ss.pgConnectFlags(cfg.DBConfig), flags...)

	if cfg.Verbose {
		cmd = append(cmd, "--verbose")
	}

	return cmd
}

// pgConnectFlags returns the command-line flags needed to connect to a PostgreSQL database.
func (ss *SnapshotService) pgConnectFlags(cfg SnapshotDatabaseConfig) []string {
	schema := cfg.SchemaName
	if schema == "" {
		schema = "public"
	}
	flags := []string{
		"--host", cfg.Host,
		"--port", fmt.Sprintf("%d", cfg.Port),
		"--dbname", cfg.DbName,
		"--schema", schema,
	}

	if cfg.User != "" {
		flags = append(flags, "--username", cfg.User)
	}

	return flags
}

// buildPostgresEnvVars returns environment variables needed for PostgreSQL operations.
// It includes password and SSL configuration variables.
func (ss *SnapshotService) buildPostgresEnvVars(cfg SnapshotDatabaseConfig) []string {
	vars := []string{}

	vars = append(vars, fmt.Sprintf("PGPASSWORD=%s", cfg.Password))

	if cfg.SSLMode == "" || cfg.SSLMode == "disable" {
		return vars
	}
	vars = append(vars, fmt.Sprintf("PGSSLMODE=%s", cfg.SSLMode))

	if cfg.SSLKey != "" {
		vars = append(vars, fmt.Sprintf("PGSSLKEY=%s", cfg.SSLKey))
	}

	if cfg.SSLCert != "" {
		vars = append(vars, fmt.Sprintf("PGSSLCERT=%s", cfg.SSLCert))
	}

	if cfg.SSLRootCert != "" {
		vars = append(vars, fmt.Sprintf("PGSSLROOTCERT=%s", cfg.SSLRootCert))
	}

	return vars
}
