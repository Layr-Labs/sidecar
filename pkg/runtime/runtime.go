package runtime

import (
	"errors"
	"github.com/Layr-Labs/sidecar/internal/config"
	"go.uber.org/zap"
	"golang.org/x/mod/semver"
	"gorm.io/gorm"
)

type SidecarRuntime struct {
	grm          *gorm.DB
	globalConfig *config.Config
	logger       *zap.Logger
}

func NewSidecarRuntime(grm *gorm.DB, globalConfig *config.Config, l *zap.Logger) *SidecarRuntime {
	return &SidecarRuntime{
		grm:          grm,
		globalConfig: globalConfig,
		logger:       l,
	}
}

func (s *SidecarRuntime) GetRecentlyLaunchedSidecarVersion() (*SidecarVersions, error) {
	var sv SidecarVersions
	res := s.grm.Model(&SidecarVersions{}).Order("id desc").First(&sv)
	if res.Error != nil {
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, res.Error
	}
	return &sv, nil
}

func (s *SidecarRuntime) InsertRecentlyLaunchedSidecarVersion(version string, stateRootBlockLaunchedAt uint64) (*SidecarVersions, error) {
	sv := &SidecarVersions{
		Version:                  version,
		StateRootBlockLaunchedAt: stateRootBlockLaunchedAt,
	}
	res := s.grm.Model(&SidecarVersions{}).Create(&sv)
	if res.Error != nil {
		return nil, res.Error
	}
	return sv, nil
}

func (s *SidecarRuntime) ValidateAndUpdateSidecarVersion(version string) error {
	if version == "" {
		return errors.New("empty version")
	}

	if version == "unknown" {
		s.logger.Sugar().Warnw("runtime version is unknown, not inserting into sidecar_versions", zap.String("version", version))
		return nil
	}

	lastSeenVersion, err := s.GetRecentlyLaunchedSidecarVersion()
	if err != nil {
		return err
	}

	if lastSeenVersion != nil {
		// new version should be >= last seen version
		cmp := semver.Compare(version, lastSeenVersion.Version)
		if cmp < 0 {
			return errors.New("runtime version is older than last seen version")
		}
		if cmp == 0 {
			s.logger.Sugar().Infow("runtime version is the same as the last seen version", zap.String("version", version))
			return nil
		}
	}

	var blockNumber uint64
	res := s.grm.Raw("SELECT MAX(eth_block_number) FROM state_roots").Scan(&blockNumber)
	if res.Error != nil && !errors.Is(res.Error, gorm.ErrRecordNotFound) {
		return res.Error
	}
	if errors.Is(res.Error, gorm.ErrRecordNotFound) {
		blockNumber = s.globalConfig.GetGenesisBlockNumber()
	}

	res = s.grm.Model(&SidecarVersions{}).Create(&SidecarVersions{
		Version:                  version,
		StateRootBlockLaunchedAt: blockNumber,
	})
	return res.Error
}
