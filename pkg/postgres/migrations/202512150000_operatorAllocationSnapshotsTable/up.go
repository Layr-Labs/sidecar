package _202512150000_operatorAllocationSnapshotsTable

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct{}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		// Create operator_allocation_snapshots table for unique stake calculations
		// Stores daily snapshots of operator allocations (magnitude) per operator set
		`CREATE TABLE IF NOT EXISTS operator_allocation_snapshots (
			operator varchar not null,
			avs varchar not null,
			strategy varchar not null,
			operator_set_id bigint not null,
			magnitude numeric not null,
			snapshot date not null,
			primary key (operator, avs, strategy, operator_set_id, snapshot)
		)`,

		// Add effective_date for allocation/deallocation rounding
		`ALTER TABLE operator_allocations 
		 ADD COLUMN IF NOT EXISTS effective_date date`,

		// Index for efficient allocation queries
		`CREATE INDEX IF NOT EXISTS idx_operator_allocations_effective_date 
		 ON operator_allocations(operator, avs, strategy, operator_set_id, effective_date)`,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202512150000_operatorAllocationSnapshotsTable"
}
