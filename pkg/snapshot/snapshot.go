package snapshot

import (
	"fmt"
	"os"

	pgcommands "github.com/habx/pg-commands"
	"go.uber.org/zap"
)

// SnapshotConfig encapsulates all configuration needed for snapshot operations.
type SnapshotConfig struct {
	OutputFile string
	InputFile  string
	Host       string
	Port       int
	User       string
	Password   string
	DbName     string
	SchemaName string
}

// SnapshotService encapsulates the configuration and logger for snapshot operations.
type SnapshotService struct {
	cfg *SnapshotConfig
	l   *zap.Logger
}

// NewSnapshotService initializes a new SnapshotService with the given configuration and logger.
func NewSnapshotService(cfg *SnapshotConfig, l *zap.Logger) *SnapshotService {
	return &SnapshotService{
		cfg: cfg,
		l:   l,
	}
}

// CreateSnapshot creates a snapshot of the database based on the provided configuration.
func (s *SnapshotService) CreateSnapshot() error {
	if !s.validateCreateSnapshotConfig() {
		return fmt.Errorf("invalid snapshot configuration")
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
func (s *SnapshotService) RestoreSnapshot() error {
	if !s.validateRestoreConfig() {
		return fmt.Errorf("invalid restore configuration")
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

func (s *SnapshotService) validateCreateSnapshotConfig() bool {
	if s.cfg.Host == "" {
		s.l.Sugar().Error("Database host is required")
		return false
	}

	if s.cfg.OutputFile == "" {
		s.l.Sugar().Error("Output path i.e. `output-file` must be specified")
		return false
	}

	return true
}

func (s *SnapshotService) setupSnapshotDump() (*pgcommands.Dump, error) {
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

func (s *SnapshotService) validateRestoreConfig() bool {
	if s.cfg.InputFile == "" {
		s.l.Sugar().Error("Restore snapshot file path i.e. `input-file` must be specified")
		return false
	}

	info, err := os.Stat(s.cfg.InputFile)
	if err != nil || info.IsDir() {
		s.l.Sugar().Errorw("Snapshot file does not exist", "path", s.cfg.InputFile)
		return false
	}

	return true
}

func (s *SnapshotService) setupRestore() (*pgcommands.Restore, error) {
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
