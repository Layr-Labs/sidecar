package _202410222022_excludedAddresses

import (
	"gorm.io/gorm"
)

type SqliteMigration struct {
}

func (m *SqliteMigration) Up(grm *gorm.DB) error {
	query := `
		create table if not exists excluded_addresses (
			address TEXT NOT NULL PRIMARY KEY,
			network TEXT NOT NULL,
			description TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT NULL,
			deleted_at DATETIME,
			unique(address)
		)
	`

	res := grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (m *SqliteMigration) GetName() string {
	return "202410222022_excludedAddresses"
}
