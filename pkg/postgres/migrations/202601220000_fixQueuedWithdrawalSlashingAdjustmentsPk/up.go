package _202601220000_fixQueuedWithdrawalSlashingAdjustmentsPk

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		// Drop the incorrect PK (block_number, log_index, transaction_hash)
		`ALTER TABLE queued_withdrawal_slashing_adjustments DROP CONSTRAINT queued_withdrawal_slashing_adjustments_pk`,
		// Drop the unique constraint
		`ALTER TABLE queued_withdrawal_slashing_adjustments DROP CONSTRAINT uniq_queued_withdrawal_slashing_adjustments`,
		// Add withdrawal_log_index column to uniquely identify each withdrawal within a block
		`ALTER TABLE queued_withdrawal_slashing_adjustments ADD COLUMN withdrawal_log_index bigint NOT NULL DEFAULT 0`,
		// Add the correct PK that allows:
		// 1. Multiple stakers per slash event
		// 2. Multiple withdrawals from the same staker in the same block
		`ALTER TABLE queued_withdrawal_slashing_adjustments ADD CONSTRAINT queued_withdrawal_slashing_adjustments_pk PRIMARY KEY (staker, strategy, operator, withdrawal_block_number, withdrawal_log_index, slash_block_number)`,
	}

	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			return res.Error
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202601220000_fixQueuedWithdrawalSlashingAdjustmentsPk"
}
