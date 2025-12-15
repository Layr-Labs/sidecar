package _202512160000_addSlashableUntilToOperatorRegistrationSnapshots

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		// Step 1: Add nullable column
		`ALTER TABLE operator_set_operator_registration_snapshots
		 ADD COLUMN IF NOT EXISTS slashable_until DATE`,

		// Step 2: Backfill accurate slashable_until dates for deregistered operators
		// NULL = operator is still active, DATE = deregistration_date + 14 days
		`WITH deregistration_events AS (
			SELECT
				osor.operator,
				osor.avs,
				osor.operator_set_id,
				DATE(b.block_time) as deregistration_date,
				DATE(b.block_time) + INTERVAL '14 days' as slashable_until_date
			FROM operator_set_operator_registrations osor
			JOIN blocks b ON osor.block_number = b.number
			WHERE osor.is_active = FALSE
		),
		last_deregistration AS (
			-- Get the most recent deregistration for each (operator, avs, operator_set_id)
			SELECT DISTINCT ON (operator, avs, operator_set_id)
				operator,
				avs,
				operator_set_id,
				deregistration_date,
				slashable_until_date
			FROM deregistration_events
			ORDER BY operator, avs, operator_set_id, deregistration_date DESC
		)
		UPDATE operator_set_operator_registration_snapshots osors
		SET slashable_until = ld.slashable_until_date
		FROM last_deregistration ld
		WHERE osors.operator = ld.operator
		  AND osors.avs = ld.avs
		  AND osors.operator_set_id = ld.operator_set_id
		  AND osors.snapshot <= ld.deregistration_date`,
	}

	for _, query := range queries {
		if _, err := db.Exec(query); err != nil {
			return err
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202512160000_addSlashableUntilToOperatorRegistrationSnapshots"
}
