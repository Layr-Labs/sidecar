package _202501071401_defaultOperatorSplitSnapshots

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS default_operator_split_snapshots (
			split integer not null,
			snapshot date not null
		)`,
	}
	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202501071401_defaultOperatorSplitSnapshots"
}
