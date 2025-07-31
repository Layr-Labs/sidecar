package _202507301346_taskMailboxTables

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	// Create executor_operator_set_registered table
	query := `
		CREATE TABLE IF NOT EXISTS executor_operator_set_registered (
			caller                   varchar not null,
			avs                      varchar not null,
			executor_operator_set_id integer not null,
			is_registered           boolean not null,
			transaction_hash        varchar not null,
			block_number            bigint not null,
			log_index               bigint not null,
			unique(transaction_hash, log_index),
			foreign key (block_number) references blocks(number) on delete cascade
		);
	`
	res := grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}

	// Create task_created table
	query = `
		CREATE TABLE IF NOT EXISTS task_created (
			creator                           varchar not null,
			task_hash                        varchar not null,
			avs                              varchar not null,
			executor_operator_set_id         integer not null,
			operator_table_reference_timestamp integer not null,
			refund_collector                 varchar not null,
			avs_fee                          varchar not null,
			task_deadline                    varchar not null,
			payload                          text not null,
			transaction_hash                 varchar not null,
			block_number                     bigint not null,
			log_index                        bigint not null,
			unique(transaction_hash, log_index),
			foreign key (block_number) references blocks(number) on delete cascade
		);
	`
	res = grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}

	// Create task_verified table
	query = `
		CREATE TABLE IF NOT EXISTS task_verified (
			aggregator               varchar not null,
			task_hash               varchar not null,
			avs                     varchar not null,
			executor_operator_set_id integer not null,
			executor_cert           text not null,
			result                  text not null,
			transaction_hash        varchar not null,
			block_number            bigint not null,
			log_index               bigint not null,
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
	return "202507301346_taskMailboxTables"
}
