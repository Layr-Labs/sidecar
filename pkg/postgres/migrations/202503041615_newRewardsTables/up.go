package _202503041615_newRewardsTables

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"database/sql"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	return nil
}

func (m *Migration) GetName() string {
	return "202503041615_newRewardsTables"
}
