package _202409191425_addRewardTypeColumn

import (
	"gorm.io/gorm"
)

type SqliteMigration struct {
}

func (m *SqliteMigration) Up(grm *gorm.DB) error {
	queries := []string{
		`create table if not exists reward_submissions_new (
			avs TEXT NOT NULL,
			reward_hash TEST NOT NULL,
			token TEXT NOT NULL,
			amount TEXT NOT NULL,
			strategy TEXT NOT NULL,
			strategy_index INTEGER NOT NULL,
			multiplier TEXT NOT NULL,
			start_timestamp DATETIME NOT NULL,
			end_timestamp DATETIME NOT NULL,
			duration INTEGER NOT NULL,
			block_number INTEGER NOT NULL,
			reward_type string,
			unique(reward_hash, strategy, block_number)
		);`,
		`insert into reward_submissions_new
			select
				avs,
				reward_hash,
				token,
				amount,
				strategy,
				strategy_index,
				multiplier,
				start_timestamp,
				end_timestamp,
				duration,
				block_number,
				CASE is_for_all
					WHEN 1 THEN 'all_stakers'
					ELSE 'avs'
				END as reward_type
			from reward_submissions;
		`,
		`drop table reward_submissions;`,
		`alter table reward_submissions_new rename to reward_submissions;`,
	}

	for _, query := range queries {
		if err := grm.Exec(query).Error; err != nil {
			return err
		}
	}
	return nil
}

func (m *SqliteMigration) GetName() string {
	return "202409191425_addRewardTypeColumn"
}
