package _202503051449_addContractTypeColumn

import (
	"database/sql"
	"fmt"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`DO $$ BEGIN
			CREATE TYPE contract_type AS ENUM ('core', 'external');
		EXCEPTION
			WHEN duplicate_object THEN null;
		END $$;`,
		`ALTER TABLE contracts ADD COLUMN IF NOT EXISTS contract_type contract_type DEFAULT 'core';`,
	}

	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			fmt.Printf("Failed to run migration query: %s - %+v\n", query, res.Error)
			return res.Error
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202503051449_addContractTypeColumn"
}
