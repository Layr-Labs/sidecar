package _202512021500_stakerSharesView

import (
	"database/sql"
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

func (m *Migration) Down(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`drop view if exists staker_share_snapshots_final`,
	}

	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			fmt.Printf("Error executing query: %s\n", query)
			return res.Error
		}
	}
	return nil
}
