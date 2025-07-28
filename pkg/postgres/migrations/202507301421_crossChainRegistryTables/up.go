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

	// Insert data into generation_reservation_created
	query = `
		insert into generation_reservation_created (avs, operator_set_id, transaction_hash, block_number, log_index)
		select
			lower(tl.output_data->>'operatorSet'->>'avs') as avs,
			cast(tl.output_data->>'operatorSet'->>'id' as bigint) as operator_set_id,
			tl.transaction_hash,
			tl.block_number,
			tl.log_index
		from transaction_logs as tl
		where
			tl.address = @crossChainRegistryAddress
			and tl.event_name = 'GenerationReservationCreated'
		order by tl.block_number asc
		on conflict do nothing
	`
	contractAddresses := cfg.GetContractsMapForChain()
	res = grm.Exec(query, sql.Named("crossChainRegistryAddress", contractAddresses.CrossChainRegistry))
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

	// Insert data into generation_reservation_removed
	query = `
		insert into generation_reservation_removed (avs, operator_set_id, transaction_hash, block_number, log_index)
		select
			lower(tl.output_data->>'operatorSet'->>'avs') as avs,
			cast(tl.output_data->>'operatorSet'->>'id' as bigint) as operator_set_id,
			tl.transaction_hash,
			tl.block_number,
			tl.log_index
		from transaction_logs as tl
		where
			tl.address = @crossChainRegistryAddress
			and tl.event_name = 'GenerationReservationRemoved'
		order by tl.block_number asc
		on conflict do nothing
	`
	res = grm.Exec(query, sql.Named("crossChainRegistryAddress", contractAddresses.CrossChainRegistry))
	return res.Error
}

func (m *Migration) GetName() string {
	return "202507301421_crossChainRegistryTables"
}
