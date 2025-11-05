package _202511051502_keyRotationScheduled

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	query := `
		CREATE TABLE IF NOT EXISTS key_rotation_scheduled (
			avs               varchar not null,
			operator_set_id   integer not null,
			operator          varchar not null,
			curve_type        varchar not null,
			old_pubkey        varchar not null,
			new_pubkey        varchar not null,
			activate_at       bigint not null,
			transaction_hash  varchar not null,
			block_number      bigint not null,
			log_index         bigint not null,
			unique(transaction_hash, log_index, block_number),
			CONSTRAINT key_rotation_scheduled_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE
		);
	`
	if err := grm.Exec(query).Error; err != nil {
		return err
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202511051502_keyRotationScheduled"
}
