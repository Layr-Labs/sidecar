package precommitProcessors

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/precommitProcessors/slashingProcessor"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func LoadPrecommitProcessors(sm *stateManager.EigenStateManager, grm *gorm.DB, l *zap.Logger, cfg *config.Config) {
	slashingProcessor.NewSlashingProcessor(sm, l, grm, cfg)
}
