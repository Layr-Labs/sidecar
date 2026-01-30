package _202512161200_addMaxMagnitudeToAllocationSnapshots

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		// Add max_magnitude column to operator_allocation_snapshots table
		// This combines allocation magnitude with max magnitude in the snapshot
		// following the pattern of operator_share_snapshots and staker_share_snapshots
		// which combine data from multiple sources that are consumed together in calculations
		`ALTER TABLE operator_allocation_snapshots
		 ADD COLUMN IF NOT EXISTS max_magnitude NUMERIC NOT NULL DEFAULT 0`,

		// Create index for max_magnitude queries for performance
		`CREATE INDEX IF NOT EXISTS idx_operator_allocation_snapshots_max_magnitude
		 ON operator_allocation_snapshots(operator, strategy, snapshot)
		 WHERE max_magnitude > 0`,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202512161200_addMaxMagnitudeToAllocationSnapshots"
}
