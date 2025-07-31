package _202507301421_crossChainRegistryTables

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	// Create generation_reservation_created table
	query := `
		CREATE TABLE IF NOT EXISTS generation_reservation_created (
			avs             varchar not null,
			operator_set_id bigint not null,
			transaction_hash varchar not null,
			block_number    bigint not null,
			log_index       bigint not null,
			unique(transaction_hash, log_index),
			foreign key (block_number) references blocks(number) on delete cascade
		);
	`
	res := grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}

	// Create generation_reservation_removed table
	query = `
		CREATE TABLE IF NOT EXISTS generation_reservation_removed (
			avs             varchar not null,
			operator_set_id bigint not null,
			transaction_hash varchar not null,
			block_number    bigint not null,
			log_index       bigint not null,
			unique(transaction_hash, log_index),
			foreign key (block_number) references blocks(number) on delete cascade
		);
	`
	res = grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}

	return nil
}

func (m *Migration) GetName() string {
	return "202507301421_crossChainRegistryTables"
}
