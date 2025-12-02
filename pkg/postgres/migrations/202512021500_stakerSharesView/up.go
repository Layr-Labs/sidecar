package _202512021500_stakerSharesView

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
		// VIEW combines base shares + withdrawal queue adjustments
		`create or replace view staker_share_snapshots_final as
		select
			coalesce(base.staker, wq.staker) as staker,
			coalesce(base.strategy, wq.strategy) as strategy,
			(coalesce(base.shares::numeric, 0) + coalesce(wq.shares::numeric, 0))::text as shares,
			coalesce(base.snapshot, wq.snapshot) as snapshot
		from staker_share_snapshots base
		full outer join withdrawal_queue_share_snapshots wq
			on base.staker = wq.staker
			and base.strategy = wq.strategy
			and base.snapshot = wq.snapshot`,
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
	return "202512021500_stakerSharesView"
}
