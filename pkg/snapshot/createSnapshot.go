package snapshot

import (
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/metrics/metricsTypes"
	"go.uber.org/zap"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func defaultDumpOptions() []string {
	return []string{
		"--no-owner",
		"--no-privileges",
		"-Fc",
		"--clean",
	}
}

type Kind string

const (
	Kind_Slim    Kind = "slim"
	Kind_Full    Kind = "full"
	Kind_Archive Kind = "archive"
)

var (
	kindFlags = map[Kind]func(schema string) []string{
		Kind_Slim: func(schema string) []string {
			return []string{
				"-T", fmt.Sprintf(`%s.gold_*`, schema),
				"-T", fmt.Sprintf(`%s.sot_*`, schema),
			}
		},
		Kind_Full: func(schema string) []string {
			return []string{
				"-T", fmt.Sprintf(`%s.sot_*`, schema),
			}
		},
		Kind_Archive: func(schema string) []string {
			return []string{}
		},
	}
)

func (ss *SnapshotService) isValidDestinationPath(destPath string) (bool, error) {
	stat, err := os.Stat(destPath)
	if err != nil {
		return false, err
	}
	if !stat.IsDir() {
		return false, fmt.Errorf("destination path is not a directory")
	}
	return true, nil
}

func (ss *SnapshotService) CreateSnapshot(cfg *CreateSnapshotConfig) (*SnapshotFile, error) {
	if !cmdExists(PgDump) {
		return nil, fmt.Errorf("pg_dump not found in PATH")
	}

	startTime := time.Now()

	if valid, err := cfg.IsValid(); !valid || err != nil {
		return nil, err
	}

	destPath := cfg.DestinationPath
	if destPath == "" {
		return nil, fmt.Errorf("destination path is required")
	}

	destPath, err := filepath.Abs(destPath)
	if err != nil {
		return nil, fmt.Errorf("error getting absolute path: %w", err)
	}

	if valid, err := ss.isValidDestinationPath(destPath); !valid || err != nil {
		return nil, fmt.Errorf("invalid destination path: %w", err)
	}

	snapshotFile := newSnapshotDumpFile(destPath, cfg.Chain.String(), cfg.SidecarVersion, cfg.DBConfig.SchemaName, cfg.Kind)

	res, err := ss.performDump(snapshotFile, cfg)
	if err != nil {
		return nil, fmt.Errorf("error performing dump: %w", err)
	}
	if res.Error != nil {
		return nil, fmt.Errorf("error creating snapshot: %s", res.Error.CmdOutput)
	}
	ss.logger.Sugar().Infow("Snapshot dump complete", zap.String("outputFile", snapshotFile.FullPath()))
	_ = ss.metricsSink.Timing(metricsTypes.Metric_Timing_CreateSnapshot, time.Since(startTime), []metricsTypes.MetricsLabel{
		{
			Name:  "chain",
			Value: cfg.Chain.String(),
		},
		{
			Name:  "sidecarVersion",
			Value: cfg.SidecarVersion,
		},
		{
			Name:  "kind",
			Value: string(cfg.Kind),
		},
	})

	ss.logger.Sugar().Infow("Generating snapshot hash", zap.String("outputFile", snapshotFile.FullPath()))
	if err := snapshotFile.GenerateAndSaveSnapshotHash(); err != nil {
		return nil, fmt.Errorf("error generating snapshot hash: %w", err)
	}
	ss.logger.Sugar().Infow("Snapshot hash generated", zap.String("outputFile", snapshotFile.FullPath()))

	if err := ss.generateMetadataFile(snapshotFile, cfg); err != nil {
		return nil, fmt.Errorf("error generating metadata file: %w", err)
	}

	return snapshotFile, nil
}

func (ss *SnapshotService) generateMetadataFile(snapshotFile *SnapshotFile, cfg *CreateSnapshotConfig) error {
	if !cfg.GenerateMetadataFile {
		ss.logger.Sugar().Infow("Skipping metadata file generation", zap.String("metadataFile", snapshotFile.MetadataFilePath()))
		return nil
	}

	ss.logger.Sugar().Infow("Generating metadata file", zap.String("metadataFile", snapshotFile.MetadataFilePath()))

	if err := snapshotFile.GenerateAndSaveMetadata(); err != nil {
		return fmt.Errorf("error generating metadata file: %w", err)
	}
	ss.logger.Sugar().Infow("Metadata file generated", zap.String("metadataFile", snapshotFile.MetadataFilePath()))

	return nil
}

func (ss *SnapshotService) performDump(snapshotFile *SnapshotFile, cfg *CreateSnapshotConfig) (*Result, error) {
	flags := defaultDumpOptions()

	flags = append(flags, kindFlags[cfg.Kind](cfg.DBConfig.SchemaName)...)

	cmdFlags := ss.buildCommand(flags, cfg.SnapshotConfig)

	res := &Result{}
	fullCmdPath, err := getCmdPath(PgDump)
	if err != nil {
		return nil, fmt.Errorf("error getting pg_dump path: %w", err)
	}

	res.FullCommand = fmt.Sprintf("%s %s", fullCmdPath, strings.Join(cmdFlags, " "))

	cmd := exec.Command(fullCmdPath, cmdFlags...)
	cmd.Env = append(cmd.Env, ss.buildPostgresEnvVars(cfg.DBConfig)...)

	ss.logger.Sugar().Infow("Starting snapshot dump",
		zap.String("fullCommand", res.FullCommand),
	)

	// Create channels for synchronization
	stdoutDone := make(chan struct{})
	stderrDone := make(chan struct{})

	stderrIn, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("error creating stderr pipe: %w", err)
	}
	go func() {
		streamErrorOutput(stderrIn, res)
		close(stderrDone)
	}()

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("error creating stdout pipe: %w", err)
	}
	go func() {
		ss.logger.Sugar().Infow("Streaming snapshot to file", zap.String("outputFile", snapshotFile.FullPath()))
		streamStdout(stdoutPipe, snapshotFile.FullPath())
		close(stdoutDone)
	}()

	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("error starting command: %w", err)
	}

	// Wait for both streams to complete
	<-stdoutDone
	<-stderrDone

	err = cmd.Wait()
	if exitError, ok := err.(*exec.ExitError); ok {
		ss.logger.Sugar().Errorw("pg_dump exited with error", zap.String("error", res.Output))
		res.Error = &ResultError{Err: err, ExitCode: exitError.ExitCode(), CmdOutput: res.Output}
	}
	return res, nil
}
