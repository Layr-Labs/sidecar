package _202501271727_operatorSetOperatorRegistrations

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	query := `
		create table if not exists operator_set_operator_registrations (
			operator varchar not null,
			avs varchar not null,
			operator_set_id bigint not null,
			is_active boolean not null,
			block_number bigint not null,
			transaction_hash varchar not null,
			log_index bigint not null,
			unique(transaction_hash, log_index, block_number),
			CONSTRAINT operator_set_operator_registrations_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE
		);
	`
	if err := grm.Exec(query).Error; err != nil {
		return err
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202501271727_operatorSetOperatorRegistrations"
}
