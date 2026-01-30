package _202512101500_queuedWithdrawalSlashingAdjustments

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
		`create table if not exists queued_withdrawal_slashing_adjustments (
    		staker varchar not null,
    		strategy varchar not null,
    		operator varchar not null,
    		withdrawal_block_number bigint not null,
    		slash_block_number bigint not null,
    		slash_multiplier numeric not null,
    		block_number bigint not null,
    		transaction_hash varchar not null,
    		log_index bigint not null
    	)`,
		`alter table queued_withdrawal_slashing_adjustments add constraint queued_withdrawal_slashing_adjustments_pk primary key (block_number, log_index, transaction_hash)`,
		`alter table queued_withdrawal_slashing_adjustments add constraint queued_withdrawal_slashing_adjustments_block_number_fk foreign key (block_number) references blocks (number) on delete cascade`,
		`alter table queued_withdrawal_slashing_adjustments add constraint uniq_queued_withdrawal_slashing_adjustments unique (staker, strategy, operator, withdrawal_block_number, slash_block_number)`,
		// Indexes following pattern from slashed_operator_shares
		`create index if not exists idx_queued_withdrawal_slashing_adjustments_staker_strategy on queued_withdrawal_slashing_adjustments(staker, strategy)`,
		`create index if not exists idx_queued_withdrawal_slashing_adjustments_withdrawal_block on queued_withdrawal_slashing_adjustments(withdrawal_block_number)`,
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
	return "202512101500_queuedWithdrawalSlashingAdjustments"
}
