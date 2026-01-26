package _202601260000_uniqueAndTotalStakeRewardSubmissions

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		// Unique stake submissions (eigenState target for UniqueStakeRewardsSubmissionCreated)
		`CREATE TABLE IF NOT EXISTS unique_stake_reward_submissions (
			avs varchar not null,
			operator_set_id bigint not null,
			reward_hash varchar not null,
			token varchar not null,
			amount numeric not null,
			strategy varchar not null,
			strategy_index integer not null,
			multiplier numeric(78) not null,
			start_timestamp timestamp(6) not null,
			end_timestamp timestamp(6) not null,
			duration bigint not null,
			block_number bigint not null,
			transaction_hash varchar not null,
			log_index bigint not null,
			UNIQUE(transaction_hash, log_index, block_number, reward_hash, strategy_index),
			CONSTRAINT unique_stake_reward_submissions_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE
		)`,

		// Total stake submissions (eigenState target for TotalStakeRewardsSubmissionCreated)
		`CREATE TABLE IF NOT EXISTS total_stake_reward_submissions (
			avs varchar not null,
			operator_set_id bigint not null,
			reward_hash varchar not null,
			token varchar not null,
			amount numeric not null,
			strategy varchar not null,
			strategy_index integer not null,
			multiplier numeric(78) not null,
			start_timestamp timestamp(6) not null,
			end_timestamp timestamp(6) not null,
			duration bigint not null,
			block_number bigint not null,
			transaction_hash varchar not null,
			log_index bigint not null,
			UNIQUE(transaction_hash, log_index, block_number, reward_hash, strategy_index),
			CONSTRAINT total_stake_reward_submissions_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE
		)`,

		`CREATE TABLE IF NOT EXISTS stake_operator_set_rewards (
			avs varchar not null,
			operator_set_id bigint not null,
			reward_hash varchar not null,
			token varchar not null,
			amount numeric not null,
			strategy varchar not null,
			strategy_index integer not null,
			multiplier numeric(78) not null,
			start_timestamp timestamp(6) not null,
			end_timestamp timestamp(6) not null,
			duration bigint not null,
			reward_type varchar not null,
			block_number bigint not null,
			block_time timestamp(6) not null,
			block_date text not null,
			CONSTRAINT uniq_stake_operator_set_rewards UNIQUE (block_number, reward_hash, strategy_index, reward_type),
			CONSTRAINT stake_operator_set_rewards_block_number_fkey FOREIGN KEY (block_number) REFERENCES blocks(number) ON DELETE CASCADE
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
	return "202601260000_uniqueAndTotalStakeRewardSubmissions"
}
