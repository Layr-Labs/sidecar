package _202503042014_stakerOperatorIndex

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	query := `create index concurrently if not exists idx_staker_operator_snapshot_operator_strategy on staker_operator (snapshot, operator, strategy);`
	res := grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202503042014_stakerOperatorIndex"
}
