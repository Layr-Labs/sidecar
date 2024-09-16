package _202409161057_avsOperatorDeltas

import (
	"fmt"
	"gorm.io/gorm"
)

type SqliteMigration struct {
}

func (m *SqliteMigration) Up(grm *gorm.DB) error {
	queries := []string{
		`create table if not exists avs_operator_state_changes (
			operator TEXT NOT NULL,
			avs TEXT NOT NULL,
			block_number INTEGER NOT NULL,
			log_index INTEGER NOT NULL,
			created_at DATETIME default current_timestamp,
			registered integer not null,
			unique(operator, avs, block_number, log_index)
		);
		`,
	}

	for _, query := range queries {
		if res := grm.Exec(query); res.Error != nil {
			fmt.Printf("Failed to execute query: %s\n", query)
			return res.Error
		}
	}
	return nil
}

func (m *SqliteMigration) GetName() string {
	return "202409161057_avsOperatorDeltas"
}
