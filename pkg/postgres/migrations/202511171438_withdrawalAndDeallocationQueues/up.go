package _202511171438_withdrawalAndDeallocationQueues

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
		// PART 4: Create withdrawal_queue_share_snapshots table for rewards calculation
		// =============================================================================

		// Table to track shares in withdrawal queue that should still earn rewards
		// Stakers continue earning while in 14-day queue because they're still taking slashing risk
		// Follows naming convention: *_snapshots suffix for snapshot tables (consistent with staker_share_snapshots, etc.)
		`create table if not exists withdrawal_queue_share_snapshots (
			staker varchar not null,
			strategy varchar not null,
			shares numeric not null,
			queued_date date not null,
			withdrawable_date date not null,
			snapshot date not null,
			primary key (staker, strategy, snapshot),
			constraint uniq_withdrawal_queue_share_snapshots unique (staker, strategy, snapshot)
		)`,

		// Index for querying withdrawal queue share snapshots by snapshot date
		`create index if not exists idx_withdrawal_queue_share_snapshots_snapshot on withdrawal_queue_share_snapshots(snapshot)`,

		// Index for staker lookups
		`create index if not exists idx_withdrawal_queue_share_snapshots_staker on withdrawal_queue_share_snapshots(staker, snapshot)`,

		// =============================================================================
		// PART 5: Create deallocation_queue_snapshots table for operator allocation tracking
		// =============================================================================

		// Table to track operator allocation decreases that haven't reached effective_date yet
		// Operators continue earning on old (higher) allocation until effective_date
		// Similar concept to withdrawal queue but for operator deallocations
		`create table if not exists deallocation_queue_snapshots (
			operator varchar not null,
			avs varchar not null,
			strategy varchar not null,
			magnitude_decrease numeric not null,
			block_date date not null,
			effective_date date not null,
			snapshot date not null,
			primary key (operator, avs, strategy, snapshot),
			constraint uniq_deallocation_queue_snapshots unique (operator, avs, strategy, snapshot)
		)`,

		// Index for querying deallocation queue snapshots by snapshot date
		`create index if not exists idx_deallocation_queue_snapshots_snapshot on deallocation_queue_snapshots(snapshot)`,

		// Index for operator lookups
		`create index if not exists idx_deallocation_queue_snapshots_operator on deallocation_queue_snapshots(operator, snapshot)`,

		// Index for AVS lookups
		`create index if not exists idx_deallocation_queue_snapshots_avs on deallocation_queue_snapshots(avs, snapshot)`,
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
	return "202511171438_withdrawalAndDeallocationQueues"
}
