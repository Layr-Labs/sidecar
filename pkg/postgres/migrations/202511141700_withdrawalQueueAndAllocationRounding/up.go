package _202511141700_withdrawalQueueAndAllocationRounding

import (
	"database/sql"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		// =============================================================================
		// PART 1: Enhance queued_slashing_withdrawals to support withdrawal queue
		// =============================================================================

		// Add timestamp columns for withdrawal queue tracking
		`alter table queued_slashing_withdrawals add column if not exists queued_timestamp timestamp`,
		`alter table queued_slashing_withdrawals add column if not exists queued_date date`,

		// Add withdrawable timing columns (queued_timestamp + 14 days)
		`alter table queued_slashing_withdrawals add column if not exists withdrawable_at timestamp`,
		`alter table queued_slashing_withdrawals add column if not exists withdrawable_date date`,

		// Add completion tracking
		`alter table queued_slashing_withdrawals add column if not exists completed boolean default false`,
		`alter table queued_slashing_withdrawals add column if not exists completion_block_number bigint`,
		`alter table queued_slashing_withdrawals add column if not exists completion_timestamp timestamp`,
		`alter table queued_slashing_withdrawals add column if not exists completion_date date`,

		// Add metadata timestamps
		`alter table queued_slashing_withdrawals add column if not exists created_at timestamp default current_timestamp`,
		`alter table queued_slashing_withdrawals add column if not exists updated_at timestamp default current_timestamp`,

		// =============================================================================
		// PART 2: Create indexes for efficient withdrawal queue queries
		// =============================================================================

		// Index for finding active withdrawals during a specific time period
		`create index if not exists idx_queued_withdrawals_earning_period on queued_slashing_withdrawals(staker, queued_date, withdrawable_date) where completed = false`,

		// Index for staker lookups
		`create index if not exists idx_queued_withdrawals_staker on queued_slashing_withdrawals(staker)`,

		// Index for operator lookups
		`create index if not exists idx_queued_withdrawals_operator on queued_slashing_withdrawals(operator)`,

		// Index for completion queries
		`create index if not exists idx_queued_withdrawals_completion on queued_slashing_withdrawals(completion_block_number, completion_timestamp) where completed = true`,

		// =============================================================================
		// PART 3: Update operator_allocations table for rounding logic
		// =============================================================================

		// Add effective_date column for allocation/deallocation rounding
		`alter table operator_allocations add column if not exists effective_date date`,

		// Add timestamp column for precise timing
		`alter table operator_allocations add column if not exists block_timestamp timestamp`,

		// Create index for effective_date queries
		`create index if not exists idx_operator_allocations_effective_date on operator_allocations(operator, avs, strategy, effective_date)`,

		// Create index for date-based lookups
		`create index if not exists idx_operator_allocations_date_lookup on operator_allocations(effective_date, operator)`,
	}

	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			fmt.Printf("Error executing query: %s\n", query)
			return res.Error
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202511141700_withdrawalQueueAndAllocationRounding"
}
