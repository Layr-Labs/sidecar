package _202502180836_snapshotUniqueConstraints

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/pkg/errors"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`alter table staker_shares add constraint uniq_staker_shares unique (staker, strategy, transaction_hash, log_index, block_number)`,
		`create table if not exists staker_share_snapshots (
			staker   varchar,
			strategy varchar,
			shares   numeric,
			snapshot date
		)`,
		`alter table staker_share_snapshots add constraint uniq_staker_share_snapshots unique (staker, strategy, snapshot)`,

		// re-add indexes that likely got nuked due to dropping and re-creating the snapshot table
		`create index if not exists idx_staker_share_snapshots_staker_strategy_snapshot on staker_share_snapshots (staker, strategy, snapshot)`,
		`create index if not exists idx_staker_share_snapshots_strategy_snapshot on staker_share_snapshots (strategy, snapshot)`,

		`alter table staker_delegation_snapshots add constraint uniq_staker_delegation_snapshots unique (staker, operator, snapshot)`,
		`create index if not exists idx_staker_delegation_snapshots_operator_snapshot on staker_delegation_snapshots (operator, snapshot)`,

		`alter table operator_share_snapshots add constraint uniq_operator_share_snapshots unique (operator, strategy, snapshot)`,

		`alter table operator_shares add constraint uniq_operator_shares unique (operator, strategy, transaction_hash, log_index, block_number)`,

		`alter table operator_pi_split_snapshots add constraint uniq_operator_pi_split_snapshots unique (operator, split, snapshot)`,

		`create table if not exists operator_directed_rewards(
			avs             varchar,
			reward_hash     varchar,
			token           varchar,
			operator        varchar,
			operator_index  integer,
			amount          numeric,
			strategy        varchar,
			strategy_index  integer,
			multiplier      numeric(78),
			start_timestamp timestamp(6),
			end_timestamp   timestamp(6),
			duration        bigint,
			block_number    bigint,
			block_time      timestamp(6),
			block_date      text
		);`,
		`alter table operator_directed_rewards add constraint uniq_operator_directed_rewards unique (avs, reward_hash, strategy_index, operator_index)`,

		`alter table operator_avs_strategy_snapshots add constraint uniq_operator_avs_strategy_snapshots unique (operator, avs, strategy, snapshot)`,

		`alter table operator_avs_registration_snapshots add constraint uniq_operator_avs_registration_snapshots unique (operator, avs, snapshot)`,

		`alter table default_operator_split_snapshots add constraint uniq_default_operator_split_snapshots unique (snapshot)`,

		`alter table combined_rewards add constraint uniq_combined_rewards unique (avs, reward_hash, strategy_index)`,

		`alter table operator_avs_split_snapshots add constraint uniq_operator_avs_split_snapshots unique (operator, avs, snapshot)`,
	}

	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			return errors.Wrapf(res.Error, "failed to execute query: %s", query)
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202502180836_snapshotUniqueConstraints"
}
