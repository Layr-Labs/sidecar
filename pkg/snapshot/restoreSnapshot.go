package snapshot

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/snapshot/snapshotManifest"
	"github.com/pkg/errors"
	"github.com/schollz/progressbar/v3"
	"go.uber.org/zap"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"slices"
	"strings"
)

func defaultRestoreOptions() []string {
	return []string{
		"--clean",
		"--no-owner",
		"--no-privileges",
		"--if-exists",
	}
}

const (
	DefaultManifestUrl = "https://sidecar.eigenlayer.xyz/snapshots/manifest.json"
)

var (
	validUrlProtocols = []string{"http", "https"}
)

func (ss *SnapshotService) downloadManifest(url string) (*snapshotManifest.SnapshotManifest, error) {
	res, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error downloading manifest: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 {
		return nil, fmt.Errorf("error downloading manifest: %s", res.Status)
	}

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading manifest body: %w", err)
	}

	manifest, err := snapshotManifest.NewSnapshotManifestFromJson(bodyBytes)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling manifest: %w", err)
	}
	return manifest, nil
}

func (ss *SnapshotService) getRestoreFileFromManifest(cfg *RestoreSnapshotConfig) (*snapshotManifest.Snapshot, error) {
	manifestUrl := cfg.ManifestUrl
	if manifestUrl == "" {
		return nil, fmt.Errorf("please provide a manifest URL or a snapshot to use")
	}

	manifest, err := ss.downloadManifest(manifestUrl)
	if err != nil {
		return nil, err
	}

	snapshot := manifest.FindSnapshot(cfg.Chain.String(), cfg.SidecarVersion, cfg.DBConfig.SchemaName, string(cfg.Kind))
	if snapshot == nil {
		return nil, fmt.Errorf("no compatible snapshot found in manifest")
	}

	return snapshot, nil
}

func downloadFileToFile(url string, dest string) error {
	// create the file, or clear out the existing file
	out, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("error creating file: %w", err)
	}
	defer out.Close()

	// download the file to the destination
	res, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("error downloading file: %w", err)
	}
	defer res.Body.Close()

	if res.StatusCode >= 400 {
		return fmt.Errorf("error downloading file: status %s", res.Status)
	}
	bar := progressbar.DefaultBytes(
		res.ContentLength,
		fmt.Sprintf("downloading %s", path.Base(dest)),
	)
	defer func() {
		// print a newline after the progress bar is done to make the output look nice
		fmt.Println()
	}()

	_, err = io.Copy(io.MultiWriter(out, bar), res.Body)
	if err != nil {
		return fmt.Errorf("error writing file: %w", err)
	}

	return nil
}

func (ss *SnapshotService) downloadSnapshot(snapshotUrl string, cfg *RestoreSnapshotConfig) (*SnapshotFile, error) {
	parsedUrl, err := url.Parse(snapshotUrl)
	if err != nil {
		return nil, fmt.Errorf("error parsing snapshot URL: %w", err)
	}

	fullFilePath := fmt.Sprintf("%s/%s", os.TempDir(), path.Base(parsedUrl.Path))

	ss.logger.Sugar().Infow("downloading snapshot",
		zap.String("url", snapshotUrl),
		zap.String("destination", fullFilePath),
	)

	if err = downloadFileToFile(snapshotUrl, fullFilePath); err != nil {
		return nil, errors.Wrap(err, "error downloading snapshot")
	}

	snapshotFile := newSnapshotFile(fullFilePath)

	if cfg.VerifySnapshotHash {
		hashFilePath := snapshotFile.HashFilePath()
		ss.logger.Sugar().Infow("downloading snapshot hash",
			zap.String("url", fmt.Sprintf("%s.%s", snapshotUrl, snapshotFile.HashExt())),
			zap.String("destination", hashFilePath),
		)
		if err := downloadFileToFile(
			fmt.Sprintf("%s.%s", snapshotUrl, snapshotFile.HashExt()),
			hashFilePath,
		); err != nil {
			return nil, errors.Wrap(err, "error downloading snapshot hash")
		}
	}

	if cfg.VerifySnapshotSignature {
		signatureFilePath := snapshotFile.SignatureFilePath()
		ss.logger.Sugar().Infow("downloading snapshot signature",
			zap.String("url", fmt.Sprintf("%s.%s", snapshotUrl, snapshotFile.SignatureExt())),
			zap.String("destination", signatureFilePath),
		)
		if err := downloadFileToFile(
			fmt.Sprintf("%s.%s", snapshotUrl, snapshotFile.SignatureExt()),
			signatureFilePath,
		); err != nil {
			return nil, errors.Wrap(err, "error downloading snapshot signature")
		}
	}

	return snapshotFile, nil
}

func (ss *SnapshotService) isUrl(input string) bool {
	parsedUrl, err := url.Parse(input)
	if err != nil {
		return false
	}
	return slices.Contains(validUrlProtocols, parsedUrl.Scheme)
}

func (ss *SnapshotService) performRestore(snapshotFile *SnapshotFile, cfg *RestoreSnapshotConfig) (*Result, error) {
	flags := defaultRestoreOptions()

	cmdFlags := ss.buildCommand(flags, cfg.SnapshotConfig)
	cmdFlags = append(cmdFlags, snapshotFile.FullPath())

	res := &Result{}
	fullCmdPath, err := getCmdPath(PgRestore)
	if err != nil {
		return nil, fmt.Errorf("error getting pg_restore path: %w", err)
	}

	res.FullCommand = fmt.Sprintf("%s %s", fullCmdPath, strings.Join(cmdFlags, " "))

	cmd := exec.Command(fullCmdPath, cmdFlags...)
	cmd.Env = append(cmd.Env, ss.buildPostgresEnvVars(cfg.DBConfig)...)

	ss.logger.Sugar().Infow("Starting snapshot restore",
		zap.String("fullCommand", res.FullCommand),
	)

	// Create channel for synchronization
	stderrDone := make(chan struct{})

	stderrIn, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("error creating stderr pipe: %w", err)
	}
	go func() {
		streamErrorOutput(stderrIn, res)
		close(stderrDone)
	}()

	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("error starting command: %w", err)
	}

	// Wait for stream to complete
	<-stderrDone

	err = cmd.Wait()
	if exitError, ok := err.(*exec.ExitError); ok {
		res.Error = &ResultError{Err: err, ExitCode: exitError.ExitCode(), CmdOutput: res.Output}
	}
	return res, nil
}

func (ss *SnapshotService) RestoreFromSnapshot(cfg *RestoreSnapshotConfig) error {
	if !cmdExists(PgRestore) {
		return fmt.Errorf("pg_restore command not found")
	}

	if valid, err := cfg.IsValid(); !valid || err != nil {
		return err
	}

	input := cfg.Input

	if input == "" {
		// If no input is provided, check for a manifest
		snapshot, err := ss.getRestoreFileFromManifest(cfg)
		if err != nil {
			ss.logger.Sugar().Errorw("error getting snapshot from manifest", zap.Error(err))
			return err
		}
		input = snapshot.Url
	}

	if input == "" {
		return fmt.Errorf("please provide a snapshot URL or path to a snapshot file")
	}

	wasDownloaded := false
	var snapshotFile *SnapshotFile
	if ss.isUrl(input) {
		wasDownloaded = true
		var err error
		snapshotFile, err = ss.downloadSnapshot(input, cfg)
		if err != nil {
			ss.logger.Sugar().Errorw("error downloading snapshot", zap.Error(err))
			return err
		}
	} else {
		snapshotFile = newSnapshotFile(input)
		ss.logger.Sugar().Infow("using local snapshot file",
			zap.String("path", snapshotFile.FullPath()),
		)
	}
	if wasDownloaded {
		defer func() {
			snapshotFile.ClearFiles()
		}()
	}

	if cfg.VerifySnapshotHash {
		ss.logger.Sugar().Infow("validating snapshot hash")
		if err := snapshotFile.ValidateHash(); err != nil {
			return errors.Wrap(err, "error validating snapshot hash")
		}
		ss.logger.Sugar().Infow("snapshot hash validated")
	}
	if cfg.VerifySnapshotSignature {
		ss.logger.Sugar().Infow("validating snapshot signature")
		if err := snapshotFile.ValidateSignature(cfg.SnapshotPublicKey); err != nil {
			return errors.Wrap(err, "error validating snapshot signature")
		}
		ss.logger.Sugar().Infow("snapshot signature validated")
	}

	res, err := ss.performRestore(snapshotFile, cfg)
	if err != nil {
		return err
	}
	if res.Error != nil {
		ss.logger.Sugar().Errorw("error restoring snapshot",
			zap.String("output", res.Error.CmdOutput),
			zap.Error(res.Error.Err),
		)
		return fmt.Errorf("error restoring snapshot %s", res.Error.CmdOutput)
	}
	return nil
}
