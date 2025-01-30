package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	pgcommands "github.com/habx/pg-commands"
	"github.com/pkg/errors"
	"github.com/schollz/progressbar/v3"
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
	return &SnapshotService{
		cfg:       cfg,
		l:         l,
		tempFiles: []string{},
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
	return u.Scheme == "http" || u.Scheme == "https"
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
	var err error

	s.cfg.OutputFile, err = resolveFilePath(s.cfg.OutputFile)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to resolve output file path '%s'", s.cfg.OutputFile))
	}

	s.l.Sugar().Debugw("Resolved file paths", zap.String("Output", s.cfg.OutputFile))

	if err := s.validateCreateSnapshotConfig(); err != nil {
		return err
	}

	dump, err := s.setupSnapshotDump()
	if err != nil {
		return err
	}

	dumpExec := dump.Exec(pgcommands.ExecOptions{StreamPrint: false})
	if dumpExec.Error != nil {
		s.l.Sugar().Errorw("Failed to create database snapshot",
			zap.Error(dumpExec.Error.Err),
			zap.String("output", dumpExec.Output),
		)
		return dumpExec.Error.Err
	}

	s.l.Sugar().Infow("Successfully created snapshot")

	outputHashFile := getHashName(s.cfg.OutputFile)
	if err := saveOutputFileHash(s.cfg.OutputFile, outputHashFile); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to save output file hash '%s'", outputHashFile))
	}

	return nil
}

// RestoreSnapshot restores a snapshot of the database based on the provided configuration.
func (s *SnapshotService) RestoreSnapshot() error {
	defer cleanup(s.tempFiles, s.l)

	if err := s.resolveRestoreInput(); err != nil {
		return err
	}

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
			zap.Error(restoreExec.Error.Err),
			zap.String("output", restoreExec.Output),
		)
		return restoreExec.Error.Err
	}

	s.l.Sugar().Infow("Successfully restored from snapshot")
	return nil
}

// resolveSnapshotServiceInput resolves the SnapshotService struct into a suitable format for restoring a snapshot
func (s *SnapshotService) resolveRestoreInput() error {
	var err error

	if isNetworkURL(s.cfg.Input) {
		inputUrl := s.cfg.Input

		// Use getFileNameFromURL to extract the file name
		fileName, err := getFileNameFromURL(inputUrl)
		if err != nil {
			return fmt.Errorf("failed to extract file name from URL: %w", err)
		}

		// Use a temporary directory for the download
		inputFilePath := filepath.Join("tmp", fileName)
		s.cfg.Input, err = downloadFile(inputUrl, inputFilePath)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to download snapshot from '%s'", inputUrl))
		}
		s.tempFiles = append(s.tempFiles, s.cfg.Input)

		if s.cfg.VerifyInput {
			hashFileName := getHashName(inputFilePath)
			hashFile, err := downloadFile(getHashName(inputUrl), hashFileName)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("failed to download snapshot hash from '%s'", hashFile))
			}
			s.tempFiles = append(s.tempFiles, hashFile)
		}
	}

	s.cfg.Input, err = resolveFilePath(s.cfg.Input)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to resolve input file path '%s'", s.cfg.Input))
	}

	s.l.Sugar().Debugw("Resolved file paths", zap.String("Input", s.cfg.Input))

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
		s.l.Sugar().Errorw("Failed to initialize pg-commands Dump", zap.Error(err))
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
		if err := validateInputFileHash(s.cfg.Input, getHashName(s.cfg.Input)); err != nil {
			return errors.Wrap(err, fmt.Sprintf("input file hash validation failed for '%s'", s.cfg.Input))
		}
		s.l.Sugar().Debugw("Input file hash validated successfully",
			zap.String("input", s.cfg.Input),
			zap.String("inputHashFile", getHashName(s.cfg.Input)),
		)
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
		s.l.Sugar().Errorw("Failed to initialize restore", zap.Error(err))
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
		return errors.Wrap(err, fmt.Sprintf("failed to compute hash of file '%s'", filePath))
	}

	fileContent := fmt.Sprintf("%s %s\n", hex.EncodeToString(hash), filepath.Base(filePath))
	if err := os.WriteFile(hashFilePath, []byte(fileContent), 0644); err != nil {
		return fmt.Errorf("failed to write hash to file: %w", err)
	}

	return nil
}

// cleanup removes the specified files and logs the results using the provided logger.
func cleanup(files []string, l *zap.Logger) {
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			l.Sugar().Warnw("Failed to remove temporary file",
				zap.String("file", file),
				zap.Error(err),
			)
		} else {
			l.Sugar().Debugw("Removed temporary file", zap.String("file", file))
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
		return errors.Wrap(err, fmt.Sprintf("failed to compute hash of input file '%s'", inputFile))
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

func downloadFile(url, filePath string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", errors.Wrap(err, "failed to initiate download")
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", errors.Wrap(fmt.Errorf("file not found at URL: %s", url), "404 Not Found")
	} else if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: received status code %d", resp.StatusCode)
	}

	// Ensure the directory for the file path exists
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return "", errors.Wrap(err, "failed to create directories for file path")
	}

	// For CLI UX Get the content length for the progress bar
	contentLength := resp.ContentLength

	// Create a progress bar with the file path
	bar := progressbar.NewOptions64(
		contentLength,
		progressbar.OptionSetDescription(fmt.Sprintf("downloading %s", filePath)),
		progressbar.OptionSetTheme(progressbar.Theme{Saucer: "=", SaucerPadding: " ", BarStart: "[", BarEnd: "]"}),
	)

	tmpFile, err := os.Create(filePath)
	if err != nil {
		return "", errors.Wrap(err, "failed to create file")
	}

	// Use io.TeeReader to update the progress bar while copying
	_, err = io.Copy(io.MultiWriter(tmpFile, bar), resp.Body)
	if err != nil {
		tmpFile.Close()
		return "", errors.Wrap(err, "failed to write to file")
	}

	tmpFile.Close()
	return filePath, nil
}

// getHashName returns the hash name for a given file path or URL.
func getHashName(input string) string {
	return input + ".sha256sum"
}

func getFileNameFromURL(rawURL string) (string, error) {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}
	return path.Base(parsedURL.Path), nil
}
