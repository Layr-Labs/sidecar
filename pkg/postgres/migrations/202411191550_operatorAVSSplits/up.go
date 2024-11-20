package _202411191550_operatorAVSSplits

import (
	"database/sql"

	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB) error {
	query := `
		create table if not exists operator_avs_splits (
			operator varchar not null,
			avs varchar not null,
			activated_at timestamp(6) not null,
			old_operator_avs_split_bips integer not null,
			new_operator_avs_split_bips integer not null,
			block_number bigint not null,
			transaction_hash varchar not null,
			log_index bigint not null,
			unique(transaction_hash, log_index, block_number),
			CONSTRAINT operator_avs_splits_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE
		);
	`
	if err := grm.Exec(query).Error; err != nil {
		return err
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202411191550_operatorAVSSplits"
}
