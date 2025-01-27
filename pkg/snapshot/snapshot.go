package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	pgcommands "github.com/habx/pg-commands"
	"go.uber.org/zap"
)

// SnapshotConfig encapsulates all configuration needed for snapshot operations.
type SnapshotConfig struct {
	OutputFile     string
	OutputHashFile string
	InputFile      string
	InputURL       string
	InputHashFile  string
	InputHashURL   string
	Host           string
	Port           int
	User           string
	Password       string
	DbName         string
	SchemaName     string
}

// SnapshotService encapsulates the configuration and logger for snapshot operations.
type SnapshotService struct {
	cfg       *SnapshotConfig
	l         *zap.Logger
	tempFiles []string
}

// NewSnapshotService initializes a new SnapshotService with the given configuration and logger.
func NewSnapshotService(cfg *SnapshotConfig, l *zap.Logger) (*SnapshotService, error) {
	var err error
	tempFiles := []string{}

	if cfg.InputFile == "" && cfg.InputURL != "" {
		cfg.InputFile, err = downloadFile(cfg.InputURL, "downloaded_snapshot")
		if err != nil {
			return nil, fmt.Errorf("failed to download snapshot: %w", err)
		}
		tempFiles = append(tempFiles, cfg.InputFile)
	}

	if cfg.InputHashFile == "" && cfg.InputHashURL != "" {
		cfg.InputHashFile, err = downloadFile(cfg.InputHashURL, "downloaded_snapshot_hash")
		if err != nil {
			return nil, fmt.Errorf("failed to download hash file: %w", err)
		}
		tempFiles = append(tempFiles, cfg.InputHashFile)
	}

	cfg.InputFile, err = resolveFilePath(cfg.InputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve input file path: %w", err)
	}
	cfg.OutputFile, err = resolveFilePath(cfg.OutputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve output file path: %w", err)
	}
	cfg.InputHashFile, err = resolveFilePath(cfg.InputHashFile)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve input hash file path: %w", err)
	}
	cfg.OutputHashFile, err = resolveFilePath(cfg.OutputHashFile)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve output hash file path: %w", err)
	}

	l.Sugar().Infow("Resolved file paths", "inputFile", cfg.InputFile, "outputFile", cfg.OutputFile, "inputHashFile", cfg.InputHashFile, "outputHashFile", cfg.OutputHashFile)

	return &SnapshotService{
		cfg:       cfg,
		l:         l,
		tempFiles: tempFiles,
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
func (s *SnapshotService) CreateSnapshot() error {
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

	if s.cfg.OutputHashFile != "" {
		if err := s.saveOutputFileHash(); err != nil {
			return fmt.Errorf("failed to save output file hash: %w", err)
		}
	}

	return nil
}

// RestoreSnapshot restores a snapshot of the database based on the provided configuration.
func (s *SnapshotService) RestoreSnapshot() error {
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

func (s *SnapshotService) validateCreateSnapshotConfig() error {
	if s.cfg.Host == "" {
		return fmt.Errorf("database host is required")
	}

	if s.cfg.OutputFile == "" {
		return fmt.Errorf("output path i.e. `output-file` must be specified")
	}

	return nil
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

func (s *SnapshotService) validateRestoreConfig() error {
	if s.cfg.InputFile == "" && s.cfg.InputURL == "" {
		return fmt.Errorf("restore snapshot file path i.e. `input-file` or `input-url` must be specified")
	}

	info, err := os.Stat(s.cfg.InputFile)
	if err != nil || info.IsDir() {
		return fmt.Errorf("snapshot file does not exist: %s", s.cfg.InputFile)
	}

	if s.cfg.InputHashFile != "" {
		if err := validateInputFileHash(s.cfg.InputFile, s.cfg.InputHashFile); err != nil {
			return fmt.Errorf("input file hash validation failed: %w", err)
		}
		s.l.Sugar().Infow("Input file hash validated successfully", "inputFile", s.cfg.InputFile, "inputHashFile", s.cfg.InputHashFile)
	}

	return nil
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

func (s *SnapshotService) saveOutputFileHash() error {
	hash, err := getFileHash(s.cfg.OutputFile)
	if err != nil {
		return fmt.Errorf("failed to compute hash of output file: %w", err)
	}

	hashString := hex.EncodeToString(hash)
	if err := os.WriteFile(s.cfg.OutputHashFile, []byte(hashString), 0644); err != nil {
		return fmt.Errorf("failed to write hash to file: %w", err)
	}

	s.l.Sugar().Infow("Output file hash saved successfully", "outputFile", s.cfg.OutputFile, "outputHashFile", s.cfg.OutputHashFile)
	return nil
}

// Cleanup removes any temporary files created during the snapshot process.
func (s *SnapshotService) Cleanup() {
	for _, file := range s.tempFiles {
		if err := os.Remove(file); err != nil {
			s.l.Sugar().Warnw("Failed to remove temporary file", "file", file, "error", err)
		} else {
			s.l.Sugar().Infow("Removed temporary file", "file", file)
		}
	}
}

// validateInputFileHash validates the InputFile against the hash in InputHashFile.
func validateInputFileHash(inputFile, hashFile string) error {
	expectedHash, err := os.ReadFile(hashFile)
	if err != nil {
		return fmt.Errorf("failed to read hash file: %w", err)
	}

	computedHash, err := getFileHash(inputFile)
	if err != nil {
		return fmt.Errorf("failed to compute hash of input file: %w", err)
	}

	if hex.EncodeToString(computedHash) != strings.TrimSpace(string(expectedHash)) {
		return fmt.Errorf("hash mismatch: expected %s, got %s", strings.TrimSpace(string(expectedHash)), hex.EncodeToString(computedHash))
	}

	return nil
}

func getFileHash(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		return nil, fmt.Errorf("failed to hash file: %w", err)
	}

	return hasher.Sum(nil), nil
}

func downloadFile(url, prefix string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: received status code %d", resp.StatusCode)
	}

	tmpFile, err := os.CreateTemp("", fmt.Sprintf("%s-*.tmp", prefix))
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}

	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		tmpFile.Close()
		return "", fmt.Errorf("failed to write to temp file: %w", err)
	}

	tmpFile.Close()
	return tmpFile.Name(), nil
}
