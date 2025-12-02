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
	// NOTE: Deallocation queue removed - delays handled by operator_allocation_snapshots (PR #474) rounding logic

	queries := []string{
		// Tracks shares in withdrawal queue earning rewards during 14-day period
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
		`create index if not exists idx_withdrawal_queue_share_snapshots_snapshot on withdrawal_queue_share_snapshots(snapshot)`,
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
	return "202511171438_withdrawalAndDeallocationQueues"
}
