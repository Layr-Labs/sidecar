package _202501301945_operatorDirectedOperatorSetRewards

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS operator_directed_operator_set_rewards (
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
			block_number bigint not null,
			block_time timestamp without time zone not null,
			block_date date not null,
			UNIQUE (avs, operator_set_id, reward_hash, operator_index, strategy_index),
			CONSTRAINT operator_directed_operator_set_rewards_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE
		)`,
	}

	for _, query := range queries {
		if err := grm.Exec(query).Error; err != nil {
			return err
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202501301945_operatorDirectedOperatorSetRewards"
}
