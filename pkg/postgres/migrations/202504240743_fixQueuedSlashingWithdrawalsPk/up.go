package _202504240743_fixQueuedSlashingWithdrawalsPk

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`alter table queued_slashing_withdrawals drop constraint queued_slashing_withdrawals_pk;`,
		`alter table queued_slashing_withdrawals add constraint queued_slashing_withdrawals_pk primary key (block_number, log_index, transaction_hash, staker, strategy, operator)`,
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
	return "202504240743_fixQueuedSlashingWithdrawalsPk"
}
