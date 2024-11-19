package _202411191710_blockNumberFkConstraint

import (
	"database/sql"
	"fmt"

	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB) error {
	queries := []string{
		`alter table operator_directed_reward_submissions add constraint operator_directed_reward_submissions_block_number_fkey foreign key (block_number) references blocks (number) on delete cascade`,
		`alter table operator_avs_splits add constraint operator_avs_splits_block_number_fkey foreign key (block_number) references blocks (number) on delete cascade`,
		`alter table operator_pi_splits add constraint operator_pi_splits_block_number_fkey foreign key (block_number) references blocks (number) on delete cascade`,
	}

	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			fmt.Printf("Failed to run migration query: %s - %+v\n", query, err)
			return err
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202411191710_blockNumberFkConstraint"
}
