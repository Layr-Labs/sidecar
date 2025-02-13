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

// isHttpURL checks if the given string is a network URL (http, https)
func isHttpURL(str string) bool {
	if str == "" {
		return false
	}
	u, err := url.ParseRequestURI(str)
	if err != nil {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

// getAbsoluteFilePath expands the ~ in file paths to the user's home directory and converts relative paths to absolute paths.
func getAbsoluteFilePath(path string) (string, error) {
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
	absoluteFilePath, err := getAbsoluteFilePath(s.cfg.OutputFile)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to resolve output file path '%s'", s.cfg.OutputFile))
	}

	s.l.Sugar().Debugw("Absolute file path", zap.String("Output", absoluteFilePath))

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

	outputHashFile := getHashName(absoluteFilePath)
	if err := saveOutputFileHash(absoluteFilePath, outputHashFile); err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to save output file hash '%s'", outputHashFile))
	}

	return nil
}

// RestoreSnapshot restores a snapshot of the database based on the provided configuration.
func (s *SnapshotService) RestoreSnapshot() error {
	defer func() {
		cleanupTempFiles(s.tempFiles, s.l)
		s.tempFiles = s.tempFiles[:0] // Clear the tempFiles slice after cleanup
	}()

	var absoluteFilePath string
	if isHttpURL(s.cfg.Input) {
		inputUrl := s.cfg.Input

		fileName, err := getFileNameFromURL(inputUrl)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to extract file name from URL '%s'", inputUrl))
		}

		inputFilePath := filepath.Join(os.TempDir(), fileName)

		// Download the snapshot file hash
		if s.cfg.VerifyInput {
			hashFilePath := getHashName(inputFilePath)
			hashUrl := getHashName(inputUrl)

			err = downloadFile(hashUrl, hashFilePath)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("error downloading snapshot hash from '%s'", hashUrl))
			}
			s.tempFiles = append(s.tempFiles, hashFilePath)
		}

		// Download the snapshot file
		err = downloadFile(inputUrl, inputFilePath)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("error downloading snapshot from '%s'", inputUrl))
		}
		s.tempFiles = append(s.tempFiles, inputFilePath)

		absoluteFilePath, err = getAbsoluteFilePath(inputFilePath)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to resolve input file path '%s'", inputFilePath))
		}
	} else {
		var err error
		absoluteFilePath, err = getAbsoluteFilePath(s.cfg.Input)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to resolve input file path '%s'", s.cfg.Input))
		}
	}

	s.l.Sugar().Debugw("absolute file path", zap.String("absoluteFilePath", absoluteFilePath))

	// validate snapshot against the hash file
	if s.cfg.VerifyInput {
		if err := validateInputFileHash(absoluteFilePath); err != nil {
			return errors.Wrap(err, fmt.Sprintf("input file hash validation failed for '%s'", absoluteFilePath))
		}
		s.l.Sugar().Debugw("Input file hash validated successfully",
			zap.String("input", absoluteFilePath),
			zap.String("inputHashFile", getHashName(absoluteFilePath)),
		)
	}

	restore, err := s.setupRestore()
	if err != nil {
		return err
	}

	restoreExec := restore.Exec(absoluteFilePath, pgcommands.ExecOptions{StreamPrint: false})
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
	absoluteFilePath, err := getAbsoluteFilePath(s.cfg.OutputFile)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to resolve output file path '%s'", s.cfg.OutputFile))
	}
	dump.SetFileName(absoluteFilePath)

	return dump, nil
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

// cleanupTempFiles removes the specified files and logs the results using the provided logger.
func cleanupTempFiles(files []string, l *zap.Logger) {
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
func validateInputFileHash(inputFile string) error {
	hashFile := getHashName(inputFile)
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

func downloadFile(url, downloadDestFilePath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return errors.Wrap(err, "failed to initiate download")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("downloading error, received status code %d", resp.StatusCode)
	}

	// Ensure the directory for the file path exists
	dir := filepath.Dir(downloadDestFilePath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return errors.Wrap(err, "failed to create directories to save the download file to")
		}
	}

	// Create a progress bar with the file path
	bar := progressbar.NewOptions64(
		resp.ContentLength,
		progressbar.OptionSetDescription(fmt.Sprintf("downloading %s", downloadDestFilePath)),
		progressbar.OptionSetTheme(progressbar.Theme{Saucer: "=", SaucerPadding: " ", BarStart: "[", BarEnd: "]"}),
	)

	tmpFile, err := os.Create(downloadDestFilePath)
	if err != nil {
		return errors.Wrap(err, "failed to create local file")
	}
	defer tmpFile.Close()

	_, err = io.Copy(io.MultiWriter(tmpFile, bar), resp.Body)
	if err != nil {
		return errors.Wrap(err, "failed to write to local file")
	}

	return nil
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
