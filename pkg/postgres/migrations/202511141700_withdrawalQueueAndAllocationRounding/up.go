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

		// Add completion tracking (timestamps can be derived from blocks table via FK)
		`alter table queued_slashing_withdrawals add column if not exists completed boolean default false`,
		`alter table queued_slashing_withdrawals add column if not exists completion_block_number bigint`,

		// Add FK constraint for completion block
		`alter table queued_slashing_withdrawals add constraint if not exists fk_completion_block foreign key (completion_block_number) references blocks(number) on delete set null`,

		// =============================================================================
		// PART 2: Create indexes for efficient withdrawal queue queries
		// =============================================================================

		// Index for finding active (non-completed) withdrawals
		`create index if not exists idx_queued_withdrawals_active on queued_slashing_withdrawals(staker, operator, completed) where completed = false`,

		// Index for completion queries
		`create index if not exists idx_queued_withdrawals_completed on queued_slashing_withdrawals(completion_block_number) where completed = true`,

		// =============================================================================
		// PART 3: Update operator_allocations table for rounding logic
		// =============================================================================

		// Add effective_date column for allocation/deallocation rounding
		// This is a computed value based on magnitude changes (round UP for increases, DOWN for decreases)
		// block_timestamp can be derived from block_number FK to blocks table
		`alter table operator_allocations add column if not exists effective_date date`,

		// Create index for effective_date queries
		`create index if not exists idx_operator_allocations_effective_date on operator_allocations(operator, avs, strategy, effective_date)`,
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
