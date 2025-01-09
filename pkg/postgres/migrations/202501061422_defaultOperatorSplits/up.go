package _202501061422_defaultOperatorSplits

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	query := `
		create table if not exists default_operator_splits (
			old_default_operator_split_bips integer not null,
			new_default_operator_split_bips integer not null,
			block_number bigint not null,
			transaction_hash varchar not null,
			log_index bigint not null,
			unique(transaction_hash, log_index, block_number),
			CONSTRAINT default_operator_splits_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE
		);
	`
	if err := grm.Exec(query).Error; err != nil {
		return err
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202501061422_defaultOperatorSplits"
}
