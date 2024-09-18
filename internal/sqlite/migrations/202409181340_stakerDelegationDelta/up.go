package _202409181340_stakerDelegationDelta

import (
	"gorm.io/gorm"
)

type SqliteMigration struct {
}

func (m *SqliteMigration) Up(grm *gorm.DB) error {
	query := `
		create table if not exists staker_delegation_changes (
			staker TEXT NOT NULL,
			operator TEXT NOT NULL,
			delegated INTEGER NOT NULL,
			block_number INTEGER NOT NULL,
			log_index INTEGER NOT NULL,
			unique(staker, operator, delegated, log_index)    
		)
	`
	res := grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (m *SqliteMigration) GetName() string {
	return "202409181340_stakerDelegationDelta"
}
