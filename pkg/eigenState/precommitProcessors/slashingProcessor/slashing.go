package slashingProcessor

import (
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type SlashingProcessor struct {
	logger *zap.Logger
	grm    *gorm.DB
}

func NewSlashingProcessor(logger *zap.Logger, grm *gorm.DB) *SlashingProcessor {
	return &SlashingProcessor{
		logger: logger,
		grm:    grm,
	}
}

func (sp *SlashingProcessor) GetName() string {
	return "slashingProcessor"
}

func (sp *SlashingProcessor) Process(blockNumber uint64, models map[string]types.IEigenStateModel) error {

}
