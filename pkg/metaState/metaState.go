package metaState

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/metaState/executorOperatorSetRegistered"
	"github.com/Layr-Labs/sidecar/pkg/metaState/generationReservationCreated"
	"github.com/Layr-Labs/sidecar/pkg/metaState/generationReservationRemoved"
	"github.com/Layr-Labs/sidecar/pkg/metaState/keyRotationScheduled"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/metaState/rewardsClaimed"
	"github.com/Layr-Labs/sidecar/pkg/metaState/taskCreated"
	"github.com/Layr-Labs/sidecar/pkg/metaState/taskVerified"
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

	if _, err := generationReservationCreated.NewGenerationReservationCreatedModel(db, l, cfg, msm); err != nil {
		l.Sugar().Errorw("Failed to create GenerationReservationCreatedModel", zap.Error(err))
		return err
	}

	if _, err := generationReservationRemoved.NewGenerationReservationRemovedModel(db, l, cfg, msm); err != nil {
		l.Sugar().Errorw("Failed to create GenerationReservationRemovedModel", zap.Error(err))
		return err
	}

	if _, err := executorOperatorSetRegistered.NewExecutorOperatorSetRegisteredModel(db, l, cfg, msm); err != nil {
		l.Sugar().Errorw("Failed to create ExecutorOperatorSetRegisteredModel", zap.Error(err))
		return err
	}

	if _, err := taskCreated.NewTaskCreatedModel(db, l, cfg, msm); err != nil {
		l.Sugar().Errorw("Failed to create TaskCreatedModel", zap.Error(err))
		return err
	}

	if _, err := taskVerified.NewTaskVerifiedModel(db, l, cfg, msm); err != nil {
		l.Sugar().Errorw("Failed to create TaskVerifiedModel", zap.Error(err))
		return err
	}

	if _, err := keyRotationScheduled.NewKeyRotationScheduledModel(db, l, cfg, msm); err != nil {
		l.Sugar().Errorw("Failed to create KeyRotationScheduledModel", zap.Error(err))
		return err
	}

	return nil
}
