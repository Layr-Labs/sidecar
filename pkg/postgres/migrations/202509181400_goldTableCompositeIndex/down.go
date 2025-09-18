package _202509181400_goldTableCompositeIndex

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

func (m *Migration) Down(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	// Drop the composite index
	query := `drop index if exists idx_gold_table_composite;`

	res := grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}
	return nil
}
