package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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
	"gorm.io/gorm"
)

// SnapshotConfig encapsulates all configuration needed for snapshot operations.
type SnapshotConfig struct {
	OutputFile  string
	Input       string
	VerifyInput bool
	ManifestURL string
	Host        string
	Port        int
	User        string
	Password    string
	DbName      string
	SchemaName  string
	Version     string
	Chain       string
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
	resolvedFilePath, err := resolveFilePath(s.cfg.OutputFile)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to resolve output file path '%s'", s.cfg.OutputFile))
	}

	s.l.Sugar().Debugw("Resolved file paths", zap.String("Output", resolvedFilePath))

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

	outputHashFile := getHashName(resolvedFilePath)
	if err := saveOutputFileHash(resolvedFilePath, outputHashFile); err != nil {
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

	var snapshotAbsoluteFilePath string
	var err error
	if s.cfg.Input == "" && s.cfg.ManifestURL == "" {
		return errors.Wrap(fmt.Errorf("input file or manifest URL is required"), "missing required configuration")
	} else if s.cfg.Input == "" && s.cfg.ManifestURL != "" {
		// Get the desired snapshot URL from the manifest
		desiredURL, err := s.getDesiredURLFromManifest(s.cfg.ManifestURL)
		if err != nil {
			return err
		}
		// Use the desired URL as the input for downloading
		snapshotAbsoluteFilePath, err = s.downloadSnapshotAndVerificationFiles(desiredURL)
		if err != nil {
			return err
		}
	} else if isHttpURL(s.cfg.Input) {
		inputUrl := s.cfg.Input
		snapshotAbsoluteFilePath, err = s.downloadSnapshotAndVerificationFiles(inputUrl)
		if err != nil {
			return err
		}
	} else {
		var err error
		snapshotAbsoluteFilePath, err = resolveFilePath(s.cfg.Input)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to resolve input file path '%s'", s.cfg.Input))
		}
	}

	s.l.Sugar().Debugw("Snapshot absolute file path", zap.String("snapshotAbsoluteFilePath", snapshotAbsoluteFilePath))

	// validate snapshot against the hash file
	if s.cfg.VerifyInput {
		if err := validateInputFileHash(snapshotAbsoluteFilePath, getHashName(snapshotAbsoluteFilePath)); err != nil {
			return errors.Wrap(err, fmt.Sprintf("input file hash validation failed for '%s'", snapshotAbsoluteFilePath))
		}
		s.l.Sugar().Debugw("Input file hash validated successfully",
			zap.String("input", snapshotAbsoluteFilePath),
			zap.String("inputHashFile", getHashName(snapshotAbsoluteFilePath)),
		)
	}

	restore, err := s.setupRestore()
	if err != nil {
		return err
	}

	restoreExec := restore.Exec(snapshotAbsoluteFilePath, pgcommands.ExecOptions{StreamPrint: false})
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

	// Add option to exclude the snapshot_restore_metadata table
	dump.Options = append(dump.Options, "--exclude-table=snapshot_restore_metadata")

	resolvedFilePath, err := resolveFilePath(s.cfg.OutputFile)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to resolve output file path '%s'", s.cfg.OutputFile))
	}
	dump.SetFileName(resolvedFilePath)

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

func downloadFile(url, downloadDestFilePath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return errors.Wrap(err, "failed to initiate download")
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return fmt.Errorf("404 Not Found")
	}

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

func (s *SnapshotService) downloadSnapshotAndVerificationFiles(inputUrl string) (string, error) {
	fileName, err := getFileNameFromURL(inputUrl)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("failed to extract file name from URL '%s'", inputUrl))
	}

	inputFilePath := filepath.Join(os.TempDir(), fileName)

	// Download the snapshot file hash
	if s.cfg.VerifyInput {
		hashFilePath := getHashName(inputFilePath)
		hashUrl := getHashName(inputUrl)

		err = downloadFile(hashUrl, hashFilePath)
		if err != nil {
			if err.Error() == "404 Not Found" {
				return "", errors.Wrap(fmt.Errorf("snapshot hash file not found at '%s'. Ensure the file exists or set --verify-input=false to skip verification", hashUrl), "snapshot hash file not found")
			}
			return "", errors.Wrap(err, fmt.Sprintf("error downloading snapshot hash from '%s'", hashUrl))
		}
		s.tempFiles = append(s.tempFiles, hashFilePath)
	}

	// Download the snapshot file
	err = downloadFile(inputUrl, inputFilePath)
	if err != nil {
		if err.Error() == "404 Not Found" {
			return "", errors.Wrap(fmt.Errorf("snapshot file not found at '%s'. Ensure the file exists", inputUrl), "snapshot file not found")
		}
		return "", errors.Wrap(err, fmt.Sprintf("error downloading snapshot from '%s'", inputUrl))
	}
	s.tempFiles = append(s.tempFiles, inputFilePath)

	resolvedFilePath, err := resolveFilePath(inputFilePath)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("failed to resolve input file path '%s'", inputFilePath))
	}

	return resolvedFilePath, nil
}

// Define a struct to match the manifest format, used in MetadataVersion v1.0.0
type Manifest struct {
	Meta      Meta       `json:"meta"`
	Snapshots []Snapshot `json:"snapshots"`
}

type Meta struct {
	MetadataVersion string `json:"metadataVersion"`
	LastRefreshed   string `json:"lastRefreshed"`
}

type Snapshot struct {
	SnapshotURL string `json:"snapshotURL"`
	Chain       string `json:"chain"`
	Version     string `json:"version"`
	Schema      string `json:"schema"`
	Timestamp   string `json:"timestamp"`
}

func (s *SnapshotService) getDesiredURLFromManifest(manifestURL string) (string, error) {
	// Fetch the snapshot URL from the manifest
	if !isHttpURL(manifestURL) {
		return "", errors.Wrap(fmt.Errorf("manifest URL must be a network URL"), "invalid manifest URL")
	}

	// Download the manifest file
	manifestFileName, err := getFileNameFromURL(manifestURL)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("failed to extract file name from manifest URL '%s'", manifestURL))
	}

	manifestFilePath := filepath.Join(os.TempDir(), manifestFileName)

	err = downloadFile(manifestURL, manifestFilePath)
	if err != nil {
		if err.Error() == "404 Not Found" {
			return "", errors.Wrap(fmt.Errorf("manifest file not found at '%s'. Ensure the file exists", manifestURL), "manifest file not found")
		}
		return "", errors.Wrap(err, fmt.Sprintf("error downloading manifest from '%s'", manifestURL))
	}

	s.tempFiles = append(s.tempFiles, manifestFilePath)
	s.l.Sugar().Infow("Downloaded manifest file", zap.String("manifestFilePath", manifestFilePath))

	// Read and parse the manifest file
	manifestData, err := os.ReadFile(manifestFilePath)
	if err != nil {
		return "", errors.Wrap(err, "failed to read manifest file")
	}

	var manifest Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return "", errors.Wrap(err, "failed to parse manifest JSON")
	}

	// Filter snapshots by version, chain, and schema
	var latestSnapshot *Snapshot
	for _, snapshot := range manifest.Snapshots {
		if snapshot.Version == s.cfg.Version && snapshot.Chain == s.cfg.Chain && snapshot.Schema == s.cfg.SchemaName {
			if latestSnapshot == nil || snapshot.Timestamp > latestSnapshot.Timestamp {
				latestSnapshot = &snapshot
			}
		}
	}

	if latestSnapshot == nil {
		return "", errors.Wrap(fmt.Errorf("no matching snapshot found for version '%s', chain '%s', and schema '%s'", s.cfg.Version, s.cfg.Chain, s.cfg.SchemaName), "no matching snapshot found")
	}

	s.l.Sugar().Debugw("Selected snapshot", zap.String("snapshotURL", latestSnapshot.SnapshotURL))

	return latestSnapshot.SnapshotURL, nil
}

type SnapshotRestoreMetadata struct {
	ID                    string `gorm:"primaryKey"`
	SnapshotRestoreStatus string `gorm:"type:varchar(255);not null"`
}

func (s *SnapshotService) GetSnapshotRestoreStatus(grm *gorm.DB) (string, error) {
	if !grm.Migrator().HasTable("snapshot_restore_metadata") {
		return "", nil
	}

	var snapshotRestoreStatus string
	err := grm.Table("snapshot_restore_metadata").Select("snapshot_restore_status").Where("id = ?", "snapshot_restore_status").Scan(&snapshotRestoreStatus).Error
	return snapshotRestoreStatus, err
}

func (s *SnapshotService) SetSnapshotRestoreStatus(grm *gorm.DB, status string) error {
	if !grm.Migrator().HasTable("snapshot_restore_metadata") {
		if err := grm.AutoMigrate(&SnapshotRestoreMetadata{}); err != nil {
			return errors.Wrap(err, "failed to create snapshot_restore_metadata table")
		}
	}

	snapshotMetadata := SnapshotRestoreMetadata{
		ID:                    "snapshot_restore_status",
		SnapshotRestoreStatus: status,
	}

	if err := grm.Table("snapshot_restore_metadata").Where("id = ?", "snapshot_restore_status").Save(&snapshotMetadata).Error; err != nil {
		return errors.Wrap(err, "failed to set snapshot restore status")
	}

	return nil
}

func (s *SnapshotService) DropAllTables(grm *gorm.DB) error {
	tables := []string{}
	if err := grm.Raw("SELECT tablename FROM pg_tables WHERE schemaname = current_schema()").Scan(&tables).Error; err != nil {
		return err
	}
	for _, table := range tables {
		if err := grm.Migrator().DropTable(table); err != nil {
			return err
		}
	}
	return nil
}
