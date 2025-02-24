package snapshot

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	pgcommands "github.com/habx/pg-commands"
	"go.uber.org/zap"
)

// OldSnapshotConfig encapsulates all configuration needed for snapshot operations.
type OldSnapshotConfig struct {
	OutputFile string
	InputFile  string
	Host       string
	Port       int
	User       string
	Password   string
	DbName     string
	SchemaName string
}

// OldSnapshotService encapsulates the configuration and logger for snapshot operations.
type OldSnapshotService struct {
	cfg *OldSnapshotConfig
	l   *zap.Logger
}

// OldNewSnapshotService initializes a new OldSnapshotService with the given configuration and logger.
func OldNewSnapshotService(cfg *OldSnapshotConfig, l *zap.Logger) (*OldSnapshotService, error) {
	var err error

	cfg.InputFile, err = resolveFilePath(cfg.InputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve input file path: %w", err)
	}
	cfg.OutputFile, err = resolveFilePath(cfg.OutputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve output file path: %w", err)
	}

	l.Sugar().Infow("Resolved file paths", "inputFile", cfg.InputFile, "outputFile", cfg.OutputFile)

	return &OldSnapshotService{
		cfg: cfg,
		l:   l,
	}, nil
}

// resolveFilePath expands the ~ in file paths to the user's home directory and converts relative paths to absolute paths.
func resolveFilePath(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	if strings.HasPrefix(path, "~/") {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("failed to get user home directory: %w", err)
		}
		path = filepath.Join(homeDir, path[2:])
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path: %w", err)
	}
	return absPath, nil
}

// CreateSnapshot creates a snapshot of the database based on the provided configuration.
func (s *OldSnapshotService) CreateSnapshot() error {
	if err := s.validateCreateSnapshotConfig(); err != nil {
		return err
	}

	dump, err := s.setupSnapshotDump()
	if err != nil {
		return err
	}

	dumpExec := dump.Exec(pgcommands.ExecOptions{StreamPrint: false})
	if dumpExec.Error != nil {
		s.l.Sugar().Errorw("Failed to create database snapshot", "error", dumpExec.Error.Err, "output", dumpExec.Output)
		return dumpExec.Error.Err
	}

	s.l.Sugar().Infow("Successfully created snapshot")
	return nil
}

// RestoreSnapshot restores a snapshot of the database based on the provided configuration.
func (s *OldSnapshotService) RestoreSnapshot() error {
	if err := s.validateRestoreConfig(); err != nil {
		return err
	}

	restore, err := s.setupRestore()
	if err != nil {
		return err
	}

	restoreExec := restore.Exec(s.cfg.InputFile, pgcommands.ExecOptions{StreamPrint: false})
	if restoreExec.Error != nil {
		s.l.Sugar().Errorw("Failed to restore from snapshot",
			"error", restoreExec.Error.Err,
			"output", restoreExec.Output,
		)
		return restoreExec.Error.Err
	}

	s.l.Sugar().Infow("Successfully restored from snapshot")
	return nil
}

func (s *OldSnapshotService) validateCreateSnapshotConfig() error {
	if s.cfg.Host == "" {
		return fmt.Errorf("database host is required")
	}

	if s.cfg.OutputFile == "" {
		return fmt.Errorf("output path i.e. `output-file` must be specified")
	}

	return nil
}

func (s *OldSnapshotService) setupSnapshotDump() (*pgcommands.Dump, error) {
	dump, err := pgcommands.NewDump(&pgcommands.Postgres{
		Host:     s.cfg.Host,
		Port:     s.cfg.Port,
		DB:       s.cfg.DbName,
		Username: s.cfg.User,
		Password: s.cfg.Password,
	})
	if err != nil {
		s.l.Sugar().Errorw("Failed to initialize pg-commands Dump", "error", err)
		return nil, err
	}

	if s.cfg.SchemaName != "" {
		dump.Options = append(dump.Options, fmt.Sprintf("--schema=%s", s.cfg.SchemaName))
	}

	dump.SetFileName(s.cfg.OutputFile)

	return dump, nil
}

func (s *OldSnapshotService) validateRestoreConfig() error {
	if s.cfg.InputFile == "" {
		return fmt.Errorf("restore snapshot file path i.e. `input-file` must be specified")
	}

	info, err := os.Stat(s.cfg.InputFile)
	if err != nil || info.IsDir() {
		return fmt.Errorf("snapshot file does not exist: %s", s.cfg.InputFile)
	}

	return nil
}

func (s *OldSnapshotService) setupRestore() (*pgcommands.Restore, error) {
	restore, err := pgcommands.NewRestore(&pgcommands.Postgres{
		Host:     s.cfg.Host,
		Port:     s.cfg.Port,
		DB:       "", // left blank to not automatically assign DB as the role
		Username: s.cfg.User,
		Password: s.cfg.Password,
	})
	if err != nil {
		s.l.Sugar().Errorw("Failed to initialize restore", "error", err)
		return nil, err
	}

	restore.Options = append(restore.Options, "--if-exists")
	restore.Options = append(restore.Options, fmt.Sprintf("--dbname=%s", s.cfg.DbName))

	if s.cfg.SchemaName != "" {
		restore.SetSchemas([]string{s.cfg.SchemaName})
	}

	return restore, nil
}
