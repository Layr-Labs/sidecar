package _202502252204_slashingModels

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		// operator sets
		`CREATE TABLE IF NOT EXISTS operator_sets (
			operator_set_id bigint not null,
			avs varchar not null,
			block_number bigint not null,
			transaction_hash varchar not null,
			log_index bigint not null,
			constraint uniq_operator_sets unique(transaction_hash, log_index, block_number),
    		constraint operator_sets_block_number_fkey foreign key (block_number) references blocks(number) on delete cascade
    	)`,
		`create index if not exists idx_operator_sets_avs on operator_sets (avs)`,
		`create index if not exists idx_operator_sets_block_number on operator_sets (block_number)`,
		// operator allocations
		`CREATE TABLE IF NOT EXISTS operator_allocations (
    		operator varchar not null,
    		strategy varchar not null,
    		magnitude numeric not null,
    		effective_block bigint not null,
			operator_set_id bigint not null,
			avs varchar not null,
			block_number bigint not null,
			transaction_hash varchar not null,
			log_index bigint not null,
			constraint uniq_operator_allocations unique(transaction_hash, log_index, block_number),
    		constraint operator_allocations_block_number_fkey foreign key (block_number) references blocks(number) on delete cascade
    	)`,
		`create index if not exists idx_operator_allocations_operator on operator_allocations (operator)`,
		`create index if not exists idx_operator_allocations_avs on operator_allocations (avs)`,
		`create index if not exists idx_operator_allocations_block_number on operator_sets (block_number)`,
		// allocation delays
		`CREATE TABLE IF NOT EXISTS operator_allocation_delays (
    		operator varchar not null,
    		effective_block bigint not null,
    		delay bigint not null,
			block_number bigint not null,
			transaction_hash varchar not null,
			log_index bigint not null,
			constraint uniq_operator_allocation_delays unique(transaction_hash, log_index, block_number),
    		constraint operator_allocation_delays_block_number_fkey foreign key (block_number) references blocks(number) on delete cascade
    	)`,
		`create index if not exists idx_operator_allocation_delays_operator on operator_allocations (operator)`,
		`create index if not exists idx_operator_allocation_delays_block_number on operator_sets (block_number)`,
		// slashed operators
		`CREATE TABLE IF NOT EXISTS slashed_operators (
    		operator varchar not null,
    		strategy varchar not null,
    		wad_slashed numeric not null,
    		description text not null,
    		operator_set_id bigint not null,
    		avs varchar not null,
    		block_number bigint not null,
    		transaction_hash varchar not null,
    		log_index bigint not null,
    		constraint uniq_slashed_operators unique(transaction_hash, log_index, block_number, operator, strategy, avs, operator_set_id),
    		constraint slashed_operators_block_number_fkey foreign key (block_number) references blocks(number) on delete cascade
    	)`,
		`create index if not exists idx_slashed_operators_operator on slashed_operators (operator)`,
		`create index if not exists idx_slashed_operators_operator_avs on slashed_operators (operator, avs)`,
		`create index if not exists idx_slashed_operators_operator_set_avs on slashed_operators (operator_set_id, avs)`,
		`create index if not exists idx_slashed_operators_block_number on slashed_operators (block_number)`,
		// encumbered magnitudes
		`CREATE TABLE IF NOT EXISTS encumbered_magnitudes (
    		operator varchar not null,
    		strategy varchar not null,
    		encumbered_magnitude numeric not null,
    		transaction_hash varchar not null,
    		log_index bigint not null,
    		block_number bigint not null,
    		constraint uniq_encumbered_magnitudes unique(transaction_hash, log_index, block_number),
    		constraint encumbered_magnitudes_block_number_fkey foreign key (block_number) references blocks(number) on delete cascade
    	)`,
		`create index if not exists idx_encumbered_magnitudes_operator on encumbered_magnitudes (operator)`,
		`create index if not exists idx_encumbered_magnitudes_operator_strategy on encumbered_magnitudes (operator, strategy)`,
		`create index if not exists idx_encumbered_magnitudes_block_number on encumbered_magnitudes (block_number)`,
		// operator max magnitudes
		`CREATE TABLE IF NOT EXISTS operator_max_magnitudes (
    		operator varchar not null,
    		strategy varchar not null,
    		max_magnitude numeric not null,
    		transaction_hash varchar not null,
    		log_index bigint not null,
    		block_number bigint not null,
    		constraint uniq_operator_max_magnitudes unique(transaction_hash, log_index, block_number),
    		constraint operator_max_magnitudes_block_number_fkey foreign key (block_number) references blocks(number) on delete cascade
    	)`,
		`create index if not exists idx_operator_max_magnitudes_operator on operator_max_magnitudes (operator)`,
		`create index if not exists idx_operator_max_magnitudes_operator_strategy on operator_max_magnitudes (operator, strategy)`,
		`create index if not exists idx_operator_max_magnitudes_block_number on operator_max_magnitudes (block_number)`,
		// slashed operator shares
		`CREATE TABLE IF NOT EXISTS slashed_operator_shares (
    		operator varchar not null,
    		strategy varchar not null,
    		total_slashed_shares numeric not null,
    		transaction_hash varchar not null,
    		log_index bigint not null,
    		block_number bigint not null,
    		constraint uniq_slashed_operator_shares unique(transaction_hash, log_index, block_number),
    		constraint slashed_operator_shares_block_number_fkey foreign key (block_number) references blocks(number) on delete cascade
    	)`,
		`create index if not exists idx_slashed_operator_shares_operator on slashed_operator_shares (operator)`,
		`create index if not exists idx_slashed_operator_shares_operator_strategy on slashed_operator_shares (operator, strategy)`,
		`create index if not exists idx_slashed_operator_shares_block_number on slashed_operator_shares (block_number)`,
	}
	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			return res.Error
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202502252204_slashingModels"
}
