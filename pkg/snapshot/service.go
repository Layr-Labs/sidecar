package snapshot

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/metrics"
	"go.uber.org/zap"
)

const (
	PgDump    = "pg_dump"
	PgRestore = "pg_restore"
)

type SnapshotDatabaseConfig struct {
	Host        string
	Port        int
	DbName      string
	User        string
	Password    string
	SchemaName  string
	SSLMode     string
	SSLKey      string
	SSLCert     string
	SSLRootCert string
}

func (sdc *SnapshotDatabaseConfig) IsValid() (bool, error) {
	if sdc.DbName == "" {
		return false, fmt.Errorf("database name is required")
	}
	return true, nil
}

type SnapshotConfig struct {
	Chain          config.Chain
	SidecarVersion string
	DBConfig       SnapshotDatabaseConfig
	Verbose        bool
}

func (sc *SnapshotConfig) IsValid() (bool, error) {
	if sc.Chain == "" {
		return false, fmt.Errorf("chain is required")
	}
	if valid, err := sc.DBConfig.IsValid(); !valid || err != nil {
		return false, fmt.Errorf("invalid database configuration: %w", err)
	}
	return true, nil
}

type CreateSnapshotConfig struct {
	SnapshotConfig
	DestinationPath      string
	GenerateMetadataFile bool
	Kind                 Kind
}

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

type RestoreSnapshotConfig struct {
	SnapshotConfig
	VerifySnapshotHash      bool
	VerifySnapshotSignature bool
	SnapshotPublicKey       string
	ManifestUrl             string
	Input                   string
	Kind                    Kind
}

func (rsc *RestoreSnapshotConfig) IsValid() (bool, error) {
	if valid, err := rsc.SnapshotConfig.IsValid(); !valid || err != nil {
		return false, err
	}
	return true, nil
}

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

type SnapshotService struct {
	logger      *zap.Logger
	metricsSink *metrics.MetricsSink
}

func NewSnapshotService(l *zap.Logger, ms *metrics.MetricsSink) *SnapshotService {
	return &SnapshotService{
		logger:      l,
		metricsSink: ms,
	}
}

func (ss *SnapshotService) buildCommand(flags []string, cfg SnapshotConfig) []string {
	cmd := append(ss.pgConnectFlags(cfg.DBConfig), flags...)

	if cfg.Verbose {
		cmd = append(cmd, "--verbose")
	}

	return cmd
}

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
