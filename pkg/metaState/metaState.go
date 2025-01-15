package metaState

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/metaState/rewardsClaimed"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func LoadMetaStateModels(
	msm *metaStateManager.MetaStateManager,
	db *gorm.DB,
	l *zap.Logger,
	cfg *config.Config,
) error {
	if _, err := rewardsClaimed.NewRewardsClaimedModel(db, l, cfg, msm); err != nil {
		l.Sugar().Errorw("Failed to create RewardsClaimedModel", zap.Error(err))
		return err
	}

	return nil
}
