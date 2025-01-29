package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	pgcommands "github.com/habx/pg-commands"
	"go.uber.org/zap"
)

// SnapshotConfig encapsulates all configuration needed for snapshot operations.
type SnapshotConfig struct {
	OutputFile  string
	Input       string
	VerifyInput bool
	Host        string
	Port        int
	User        string
	Password    string
	DbName      string
	SchemaName  string
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

	if isNetworkURL(cfg.Input) {
		inputUrl := cfg.Input

		uniqueID := fmt.Sprintf("%d", time.Now().UnixNano())
		inputFileName := uniqueID + "_downloaded_snapshot.dump"

		cfg.Input, err = downloadFile(inputUrl, inputFileName)
		if err != nil {
			return nil, fmt.Errorf("failed to download snapshot: %w", err)
		}
		tempFiles = append(tempFiles, cfg.Input)

		if cfg.VerifyInput {
			hashFileName := inputFileName + ".sha256sum"
			hashFile, err := downloadFile(inputUrl+".sha256sum", hashFileName)
			if err != nil {
				return nil, fmt.Errorf("failed to download snapshot hash: %w", err)
			}
			tempFiles = append(tempFiles, hashFile)
		}
	}

	cfg.Input, err = resolveFilePath(cfg.Input)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve input file path: %w", err)
	}
	cfg.OutputFile, err = resolveFilePath(cfg.OutputFile)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve output file path: %w", err)
	}

	l.Debug(
		"Resolved file paths",
		zap.String("Input", cfg.Input),
		zap.String("OutputFile", cfg.OutputFile),
	)

	return &SnapshotService{
		cfg:       cfg,
		l:         l,
		tempFiles: tempFiles,
	}, nil
}

// isNetworkURL checks if the given string is a network URL (http, https, ftp).
// Note: This function does not allow 'file' URLs as it's ambiguous with local file.
func isNetworkURL(str string) bool {
	if str == "" {
		return false
	}
	u, err := url.ParseRequestURI(str)
	if err != nil {
		return false
	}
	// Allow HTTP, HTTPS, and FTP URL schemes
	return u.Scheme == "http" || u.Scheme == "https" || u.Scheme == "ftp"
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

	outputHashFile := s.cfg.OutputFile + ".sha256sum"
	if err := saveOutputFileHash(s.cfg.OutputFile, outputHashFile); err != nil {
		return fmt.Errorf("failed to save output file hash: %w", err)
	}

	return nil
}

// RestoreSnapshot restores a snapshot of the database based on the provided configuration.
func (s *SnapshotService) RestoreSnapshot() error {
	defer cleanup(s.tempFiles, s.l)

	if err := s.validateRestoreConfig(); err != nil {
		return err
	}

	restore, err := s.setupRestore()
	if err != nil {
		return err
	}

	restoreExec := restore.Exec(s.cfg.Input, pgcommands.ExecOptions{StreamPrint: false})
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
	if s.cfg.Input == "" {
		return fmt.Errorf("restore snapshot file path i.e. `input` must be specified")
	}

	info, err := os.Stat(s.cfg.Input)
	if err != nil || info.IsDir() {
		return fmt.Errorf("snapshot file does not exist: %s", s.cfg.Input)
	}

	if s.cfg.VerifyInput {
		if err := validateInputFileHash(s.cfg.Input, s.cfg.Input+".sha256sum"); err != nil {
			return fmt.Errorf("input file hash validation failed: %w", err)
		}
		s.l.Sugar().Infow("Input file hash validated successfully", "input", s.cfg.Input, "inputHashFile", s.cfg.Input+".sha256sum")
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

// saveOutputFileHash computes the hash of the given file and writes it to the specified hash file.
func saveOutputFileHash(filePath, hashFilePath string) error {
	hash, err := getFileHash(filePath)
	if err != nil {
		return fmt.Errorf("failed to compute hash of file: %w", err)
	}

	fileContent := fmt.Sprintf("%s %s\n", hex.EncodeToString(hash), filepath.Base(filePath))
	if err := os.WriteFile(hashFilePath, []byte(fileContent), 0644); err != nil {
		return fmt.Errorf("failed to write hash to file: %w", err)
	}

	fmt.Printf("Hash file created at: %s\n", hashFilePath)
	return nil
}

// cleanup removes the specified files and logs the results using the provided logger.
func cleanup(files []string, l *zap.Logger) {
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			l.Sugar().Warnw("Failed to remove temporary file", "file", file, "error", err)
		} else {
			l.Sugar().Infow("Removed temporary file", "file", file)
		}
	}
}

// validateInputFileHash validates the InputFile against the hash in InputHashFile.
func validateInputFileHash(inputFile, hashFile string) error {
	hashFileContent, err := os.ReadFile(hashFile)
	if err != nil {
		return fmt.Errorf("failed to read hash file: %w", err)
	}

	// hash content is expected to be in the format "<hash> <filename>"
	hashFileHash := strings.Fields(string(hashFileContent))[0]

	inputFileHash, err := getFileHash(inputFile)
	if err != nil {
		return fmt.Errorf("failed to compute hash of input file: %w", err)
	}

	if hex.EncodeToString(inputFileHash) != hashFileHash {
		return fmt.Errorf("hash mismatch: expected %s, got %s", hashFileHash, hex.EncodeToString(inputFileHash))
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

func downloadFile(url, fileName string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: received status code %d", resp.StatusCode)
	}

	tmpFile, err := os.Create(fileName)
	if err != nil {
		return "", fmt.Errorf("failed to create file: %w", err)
	}

	_, err = io.Copy(tmpFile, resp.Body)
	if err != nil {
		tmpFile.Close()
		return "", fmt.Errorf("failed to write to file: %w", err)
	}

	tmpFile.Close()
	return tmpFile.Name(), nil
}
