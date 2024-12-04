package _202412091100_fixOperatorPiSplitsFields

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`alter table operator_pi_splits rename column old_operator_avs_split_bips to old_operator_pi_split_bips`,
		`alter table operator_pi_splits rename column new_operator_avs_split_bips to new_operator_pi_split_bips`,
	}

	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			return res.Error
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202412091100_fixOperatorPiSplitsFields"
}
