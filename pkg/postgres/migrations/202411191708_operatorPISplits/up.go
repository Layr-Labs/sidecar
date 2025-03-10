package _202411191708_operatorPISplits

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"

	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	query := `
		create table if not exists operator_pi_splits (
			operator varchar not null,
			activated_at timestamp(6) not null,
			old_operator_avs_split_bips integer not null,
			new_operator_avs_split_bips integer not null,
			block_number bigint not null,
			transaction_hash varchar not null,
			log_index bigint not null,
			unique(transaction_hash, log_index, block_number),
			CONSTRAINT operator_pi_splits_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE
		);
	`
	if err := grm.Exec(query).Error; err != nil {
		return err
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202411191708_operatorPISplits"
}
