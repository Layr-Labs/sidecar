package _202501301502_operatorSetOperatorRegistrationSnapshots

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS operator_set_operator_registration_snapshots (
			operator varchar not null,
			avs varchar not null,
			operator_set_id bigint not null,
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
	return "202501301502_operatorSetOperatorRegistrationSnapshots"
}
