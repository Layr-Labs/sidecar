package _202511171438_withdrawalQueueShareSnapshots

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
	return "202511171438_withdrawalQueueShareSnapshots"
}
