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

	// Insert data into executor_operator_set_registered
	query = `
		insert into executor_operator_set_registered (caller, avs, executor_operator_set_id, is_registered, transaction_hash, block_number, log_index)
		select
			lower(tl.arguments #>> '{0, Value}') as caller,
			lower(tl.arguments #>> '{1, Value}') as avs,
			cast(tl.arguments #>> '{2, Value}' as integer) as executor_operator_set_id,
			cast(tl.output_data->>'isRegistered' as boolean) as is_registered,
			tl.transaction_hash,
			tl.block_number,
			tl.log_index
		from transaction_logs as tl
		where
			tl.address = @taskMailboxAddress
			and tl.event_name = 'ExecutorOperatorSetRegistered'
		order by tl.block_number asc
		on conflict do nothing
	`
	contractAddresses := cfg.GetContractsMapForChain()
	res = grm.Exec(query, sql.Named("taskMailboxAddress", contractAddresses.TaskMailbox))
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

	// Insert data into task_created
	query = `
		insert into task_created (creator, task_hash, avs, executor_operator_set_id, operator_table_reference_timestamp, refund_collector, avs_fee, task_deadline, payload, transaction_hash, block_number, log_index)
		select
			lower(tl.arguments #>> '{0, Value}') as creator,
			lower(tl.arguments #>> '{1, Value}') as task_hash,
			lower(tl.arguments #>> '{2, Value}') as avs,
			cast(tl.output_data->>'executorOperatorSetId' as integer) as executor_operator_set_id,
			cast(tl.output_data->>'operatorTableReferenceTimestamp' as integer) as operator_table_reference_timestamp,
			lower(tl.output_data->>'refundCollector') as refund_collector,
			tl.output_data->>'avsFee' as avs_fee,
			tl.output_data->>'taskDeadline' as task_deadline,
			tl.output_data->>'payload' as payload,
			tl.transaction_hash,
			tl.block_number,
			tl.log_index
		from transaction_logs as tl
		where
			tl.address = @taskMailboxAddress
			and tl.event_name = 'TaskCreated'
		order by tl.block_number asc
		on conflict do nothing
	`
	res = grm.Exec(query, sql.Named("taskMailboxAddress", contractAddresses.TaskMailbox))
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

	// Insert data into task_verified
	query = `
		insert into task_verified (aggregator, task_hash, avs, executor_operator_set_id, executor_cert, result, transaction_hash, block_number, log_index)
		select
			lower(tl.arguments #>> '{0, Value}') as aggregator,
			lower(tl.arguments #>> '{1, Value}') as task_hash,
			lower(tl.arguments #>> '{2, Value}') as avs,
			cast(tl.output_data->>'executorOperatorSetId' as integer) as executor_operator_set_id,
			tl.output_data->>'executorCert' as executor_cert,
			tl.output_data->>'result' as result,
			tl.transaction_hash,
			tl.block_number,
			tl.log_index
		from transaction_logs as tl
		where
			tl.address = @taskMailboxAddress
			and tl.event_name = 'TaskVerified'
		order by tl.block_number asc
		on conflict do nothing
	`
	res = grm.Exec(query, sql.Named("taskMailboxAddress", contractAddresses.TaskMailbox))
	return res.Error
}

func (m *Migration) GetName() string {
	return "202507301346_taskMailboxTables"
}
