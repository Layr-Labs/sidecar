package _202501241322_operatorDirectedOperatorSetRewardSubmissions

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	query := `
		create table if not exists operator_directed_operator_set_reward_submissions (
			avs varchar not null,
			operator_set_id bigint not null,
			reward_hash varchar not null,
			token varchar not null,
			operator varchar not null,
			operator_index integer not null,
			amount numeric not null,
			strategy varchar not null,
			strategy_index integer not null,
			multiplier numeric(78) not null,
			start_timestamp timestamp(6) not null,
			end_timestamp timestamp(6) not null,
			duration bigint not null,
			description varchar not null,
			block_number bigint not null,
			transaction_hash varchar not null,
			log_index bigint not null,
			unique(transaction_hash, log_index, block_number, reward_hash, strategy_index, operator_index),
			CONSTRAINT operator_directed_operator_set_reward_submissions_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE
		);
	`
	if err := grm.Exec(query).Error; err != nil {
		return err
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202501241322_operatorDirectedOperatorSetRewardSubmissions"
}
