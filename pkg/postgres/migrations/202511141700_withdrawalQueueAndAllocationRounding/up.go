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
		`alter table queued_slashing_withdrawals add column if not exists completion_block_number bigint`,

		// Add FK constraint for completion block
		`alter table queued_slashing_withdrawals add constraint fk_completion_block foreign key (completion_block_number) references blocks(number) on delete set null`,

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
			snapshot date not null
		)`,
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
