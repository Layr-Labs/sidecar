package _202503061223_renameConstraint

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	query := `alter table staker_share_deltas rename constraint uniq_staker_share_delta to uniq_staker_share_deltas`

	res := grm.Exec(query)
	return res.Error

}

func (m *Migration) GetName() string {
	return "202503061223_renameConstraint"
}
