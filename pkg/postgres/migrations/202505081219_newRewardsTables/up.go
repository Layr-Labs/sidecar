package _202505081219_newRewardsTables

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	return nil
}

func (m *Migration) GetName() string {
	return "202505081219_newRewardsTables"
}
