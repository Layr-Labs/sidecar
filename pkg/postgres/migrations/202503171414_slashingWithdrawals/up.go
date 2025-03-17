package _202503171414_slashingWithdrawals

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
		`create table if not exists queued_slashing_withdrawals (
    		staker varchar not null,
    		operator varchar not null,
    		withdrawer varchar not null,
    		nonce varchar not null,
    		start_block bigint not null,
    		strategy varchar not null,
    		scaled_shares numeric not null,
    		shares_to_withdraw numeric not null,
    		withdrawal_root text not null,
    		block_number bigint not null,
    		transaction_hash varchar not null,
    		log_index bigint not null
    	)`,
		`alter table queued_slashing_withdrawals add constraint queued_slashing_withdrawals_pk primary key (block_number, log_index, transaction_hash)`,
		`alter table queued_slashing_withdrawals add constraint queued_slashing_withdrawals_block_number_fk foreign key (block_number) references blocks (number) on delete cascade`,
		`alter table queued_slashing_withdrawals add constraint uniq_queued_slashing_withdrawals unique (block_number, log_index, transaction_hash, staker, strategy, operator)`,
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
	return "202503171414_slashingWithdrawals"
}
