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
		// PART 1: Withdrawal queue - no schema changes needed
		// =============================================================================
		// Note: Withdrawal queue logic uses withdrawable_date to determine when
		// shares should stop earning rewards. The withdrawable_date is calculated as
		// queued_date + 14 days. No additional columns needed in queued_slashing_withdrawals.

		// =============================================================================
		// PART 2: Create operator_allocation_snapshots table for rewards calculation
		// =============================================================================

		// Create snapshot table following the same pattern as staker_share_snapshots
		// This table will be populated during rewards calculation with rounding logic applied
		`CREATE TABLE IF NOT EXISTS operator_allocation_snapshots (
			operator varchar not null,
			avs varchar not null,
			strategy varchar not null,
			operator_set_id bigint not null,
			magnitude numeric not null,
			max_magnitude numeric not null,
			snapshot date not null,
			primary key (operator, avs, strategy, operator_set_id, snapshot)
		)`,

		// Add max_magnitude column if table already exists without it
		`ALTER TABLE operator_allocation_snapshots ADD COLUMN IF NOT EXISTS max_magnitude numeric NOT NULL DEFAULT '0'`,

		// =============================================================================
		// PART 3: Update operator_allocations table for allocation/deallocation rounding
		// =============================================================================

		// Add effective_date column for allocation/deallocation rounding
		// This is a computed value based on magnitude changes (round UP for increases, DOWN for decreases)
		// block_timestamp can be derived from block_number FK to blocks table
		`alter table operator_allocations add column if not exists effective_date date`,

		// Create index for effective_date queries (includes operator_set_id for proper partitioning)
		`create index if not exists idx_operator_allocations_effective_date on operator_allocations(operator, avs, strategy, operator_set_id, effective_date)`,
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
