package _202503130907_pectraPrunePartTwo

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	if cfg.Chain != config.Chain_Holesky {
		return nil
	}

	res := grm.Exec(`delete from state_roots where eth_block_number > 3419700`)
	return res.Error
}

func (m *Migration) GetName() string {
	return "202503130907_pectraPrunePartTwo"
}
