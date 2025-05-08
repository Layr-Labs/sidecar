package _202505081218_migrateRewardsTables

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/postgres/helpers"
	"gorm.io/gorm"
	"regexp"
	"strings"
)

type Migration struct {
}

type SubMigration struct {
	CreateTableQuery        string
	NewTableName            string
	ExistingTablePattern    string
	PreDataMigrationQueries []string
}

func getMinGeneratedRewardsSnapshotId(grm *gorm.DB) (uint64, error) {
	var minSnapshotId uint64
	res := grm.Raw("select min(id) from generated_rewards_snapshots").Scan(&minSnapshotId)
	if res.Error != nil {
		return 0, res.Error
	}
	return minSnapshotId, nil
}

func (sm *SubMigration) Run(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	res := grm.Exec(sm.CreateTableQuery)
	if res.Error != nil {
		fmt.Printf("Failed to execute query: %s\n", sm.CreateTableQuery)
		return res.Error
	}

	for _, query := range sm.PreDataMigrationQueries {
		res := grm.Exec(query)
		if res.Error != nil {
			fmt.Printf("Failed to execute query: %s\n", query)
			return res.Error
		}
	}

	likeTablesQuery := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
			AND table_type='BASE TABLE'
			and table_name ~* @pattern
	`
	var tables []string
	res = grm.Raw(likeTablesQuery, sql.Named("pattern", sm.ExistingTablePattern)).Scan(&tables)
	if res.Error != nil {
		fmt.Printf("Failed to find tables: %s\n%s\n", sm.ExistingTablePattern, likeTablesQuery)
		return res.Error
	}

	for _, table := range tables {
		// find the corresponding generated rewards entry
		re := regexp.MustCompile(`(202[0-9]_[0-9]{2}_[0-9]{2})$`)

		// Find the first match
		match := re.FindStringSubmatch(table)
		if len(match) != 2 {
			return fmt.Errorf("Failed to find date in table name: %s", table)
		}
		date := match[1]
		kebabDate := strings.ReplaceAll(date, "_", "-")

		generatedQuery := `select id from generated_rewards_snapshots where snapshot_date = @snapshot`
		var snapshotId uint64
		res = grm.Raw(generatedQuery, sql.Named("snapshot", kebabDate)).Scan(&snapshotId)
		if res.Error != nil && !errors.Is(res.Error, gorm.ErrRecordNotFound) {
			fmt.Printf("Failed to find generated rewards snapshot for date: %s\n", date)
			return res.Error
		}
		if errors.Is(res.Error, gorm.ErrRecordNotFound) {
			fmt.Printf("No generated rewards snapshot found for date: %s\n", date)
			var err error
			snapshotId, err = getMinGeneratedRewardsSnapshotId(grm)
			if err != nil {
				snapshotId = 0
			}
		}

		query := fmt.Sprintf("insert into %s select * from %s on conflict on constraint uniq_%s do nothing", sm.NewTableName, table, sm.NewTableName)
		res := grm.Exec(query)
		if res.Error != nil {
			fmt.Printf("Failed to execute query: %s\n", query)
			return res.Error
		}

		if snapshotId == 0 {
			continue
		}
		updateQuery := fmt.Sprintf("update %s set generated_rewards_snapshot_id = %d where generated_rewards_snapshot_id is null", sm.NewTableName, snapshotId)
		res = grm.Exec(updateQuery)
		if res.Error != nil {
			fmt.Printf("Failed to execute query: %s\n", updateQuery)
			return res.Error
		}
	}

	return nil
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	subMigrations := []SubMigration{
		{
			CreateTableQuery: `create table if not exists rewards_gold_1_active_rewards (
				avs                    varchar,
				snapshot               date,
				token                  varchar,
				tokens_per_day         double precision,
				tokens_per_day_decimal numeric,
				multiplier             numeric(78),
				strategy               varchar,
				reward_hash            varchar,
				reward_type            varchar,
				reward_submission_date text,
  				generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_1_active_rewards add constraint uniq_rewards_gold_1_active_rewards unique (avs, reward_hash, strategy, snapshot);`,
			},
			NewTableName:         "rewards_gold_1_active_rewards",
			ExistingTablePattern: "gold_[0-9]+_active_rewards_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_2_staker_reward_amounts (
				reward_hash                  varchar,
				snapshot                     date,
				token                        varchar,
				tokens_per_day               double precision,
				tokens_per_day_decimal       numeric,
				avs                          varchar,
				strategy                     varchar,
				multiplier                   numeric(78),
				reward_type                  varchar,
				reward_submission_date       text,
				operator                     varchar,
				staker                       varchar,
				shares                       numeric,
				staker_weight                numeric,
				rn                           bigint,
				total_weight                 numeric,
				staker_proportion            numeric,
				total_staker_operator_payout numeric,
				operator_tokens              numeric,
				staker_tokens                numeric,
    			generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_2_staker_reward_amounts add constraint uniq_rewards_gold_2_staker_reward_amounts unique (reward_hash, staker, avs, strategy, snapshot);`,
			},
			NewTableName:         "rewards_gold_2_staker_reward_amounts",
			ExistingTablePattern: "gold_[0-9]+_staker_reward_amounts_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_3_operator_reward_amounts (
				reward_hash     varchar,
				snapshot        date,
				token           varchar,
				tokens_per_day  double precision,
				avs             varchar,
				strategy        varchar,
				multiplier      numeric(78),
				reward_type     varchar,
				operator        varchar,
				operator_tokens numeric,
				rn              bigint,
    			generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			)`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_3_operator_reward_amounts add constraint uniq_rewards_gold_3_operator_reward_amounts unique (reward_hash, avs, operator, strategy, snapshot);`,
			},
			NewTableName:         "rewards_gold_3_operator_reward_amounts",
			ExistingTablePattern: "gold_[0-9]+_operator_reward_amounts_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_4_rewards_for_all (
				reward_hash         varchar,
				snapshot            date,
				token               varchar,
				tokens_per_day      double precision,
				avs                 varchar,
				strategy            varchar,
				multiplier          numeric(78),
				reward_type         varchar,
				staker              varchar,
				shares              numeric,
				staker_weight       numeric,
				rn                  bigint,
				total_staker_weight numeric,
				staker_proportion   numeric,
				staker_tokens       numeric(38),
    			generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			)`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_4_rewards_for_all add constraint uniq_rewards_gold_4_rewards_for_all unique (reward_hash, avs, staker, strategy, snapshot);`,
			},
			NewTableName:         "rewards_gold_4_rewards_for_all",
			ExistingTablePattern: "gold_[0-9]+_rewards_for_all_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_5_rfae_stakers (
				reward_hash                  varchar,
				snapshot                     date,
				token                        varchar,
				tokens_per_day_decimal       numeric,
				avs                          varchar,
				strategy                     varchar,
				multiplier                   numeric(78),
				reward_type                  varchar,
				reward_submission_date       text,
				operator                     varchar,
				staker                       varchar,
				shares                       numeric,
				excluded_address             varchar,
				staker_weight                numeric,
				rn                           bigint,
				total_weight                 numeric,
				staker_proportion            numeric,
				total_staker_operator_payout numeric,
				operator_tokens              numeric,
				staker_tokens                numeric,
    			generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			)`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_5_rfae_stakers add constraint uniq_rewards_gold_5_rfae_stakers unique (reward_hash, avs, staker, operator, strategy, snapshot);`,
			},
			NewTableName:         "rewards_gold_5_rfae_stakers",
			ExistingTablePattern: "gold_[0-9]+_rfae_stakers_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_6_rfae_operators (
				reward_hash            varchar,
				snapshot               date,
				token                  varchar,
				tokens_per_day_decimal numeric,
				avs                    varchar,
				strategy               varchar,
				multiplier             numeric(78),
				reward_type            varchar,
				operator               varchar,
				operator_tokens        numeric,
				rn                     bigint,
    			generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_6_rfae_operators add constraint uniq_rewards_gold_6_rfae_operators unique (reward_hash, avs, operator, strategy, snapshot);`,
			},
			NewTableName:         "rewards_gold_6_rfae_operators",
			ExistingTablePattern: "gold_[0-9]+_rfae_operators_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_7_active_od_rewards (
				avs varchar,
				operator varchar,
				snapshot date,
				token varchar,
				amount_decimal numeric,
				multiplier numeric(78),
				strategy varchar,
				duration bigint,
				reward_hash varchar,
				reward_submission_date text,
				num_registered_snapshots bigint,
				tokens_per_registered_snapshot_decimal numeric,
    			generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_7_active_od_rewards add constraint uniq_rewards_gold_7_active_od_rewards unique (reward_hash, avs, operator, strategy, snapshot);`,
			},
			NewTableName:         "rewards_gold_7_active_od_rewards",
			ExistingTablePattern: "gold_[0-9]+_active_od_rewards_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_8_operator_od_reward_amounts (
				reward_hash                            varchar,
				snapshot                               date,
				token                                  varchar,
				tokens_per_registered_snapshot_decimal numeric,
				avs                                    varchar,
				operator                               varchar,
				strategy                               varchar,
				multiplier                             numeric(78),
				reward_submission_date                 text,
				rn                                     bigint,
				split_pct                              numeric,
				operator_tokens                        numeric,
    			generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_8_operator_od_reward_amounts add constraint uniq_rewards_gold_8_operator_od_reward_amounts unique (reward_hash, avs, operator, strategy, snapshot);`,
			},
			NewTableName:         "rewards_gold_8_operator_od_reward_amounts",
			ExistingTablePattern: "gold_[0-9]+_operator_od_reward_amounts_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_9_staker_od_reward_amounts (
				reward_hash                            varchar,
				snapshot                               date,
				token                                  varchar,
				tokens_per_registered_snapshot_decimal numeric,
				avs                                    varchar,
				operator                               varchar,
				strategy                               varchar,
				multiplier                             numeric(78),
				reward_submission_date                 text,
				staker_split                           numeric,
				staker                                 varchar,
				shares                                 numeric,
				staker_weight                          numeric,
				rn                                     bigint,
				total_weight                           numeric,
				staker_proportion                      numeric,
				staker_tokens                          numeric,
    			generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_9_staker_od_reward_amounts add constraint uniq_rewards_gold_9_staker_od_reward_amounts unique (reward_hash, avs, operator, staker, strategy, snapshot);`,
			},
			NewTableName:         "rewards_gold_9_staker_od_reward_amounts",
			ExistingTablePattern: "gold_[0-9]+_staker_od_reward_amounts_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_10_avs_od_reward_amounts (
				reward_hash varchar,
				snapshot    date,
				token       varchar,
				avs         varchar,
				operator    varchar,
				avs_tokens  numeric,
				generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_10_avs_od_reward_amounts add constraint uniq_rewards_gold_10_avs_od_reward_amounts unique (reward_hash, avs, operator, snapshot);`,
			},
			NewTableName:         "rewards_gold_10_avs_od_reward_amounts",
			ExistingTablePattern: "gold_[0-9]+_staker_avs_od_reward_amounts_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_11_active_od_operator_set_rewards (
				avs                                    varchar,
				operator_set_id                        bigint,
				operator                               varchar,
				snapshot                               date,
				token                                  varchar,
				amount_decimal                         numeric,
				multiplier                             numeric(78),
				strategy                               varchar,
				duration                               bigint,
				reward_hash                            varchar,
				reward_submission_date                 text,
				num_registered_snapshots               bigint,
				tokens_per_registered_snapshot_decimal numeric,
    			generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_11_active_od_operator_set_rewards add constraint uniq_rewards_gold_11_active_od_operator_set_rewards unique (reward_hash, operator_set_id, strategy, snapshot);`,
			},
			NewTableName:         "rewards_gold_11_active_od_operator_set_rewards",
			ExistingTablePattern: "gold_[0-9]+_active_od_operator_set_rewards_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_12_operator_od_operator_set_reward_amounts (
				reward_hash                            varchar,
				snapshot                               date,
				token                                  varchar,
				tokens_per_registered_snapshot_decimal numeric,
				avs                                    varchar,
				operator_set_id                        bigint,
				operator                               varchar,
				strategy                               varchar,
				multiplier                             numeric(78),
				reward_submission_date                 text,
				rn                                     bigint,
				split_pct                              numeric,
				operator_tokens                        numeric,
    			generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_12_operator_od_operator_set_reward_amounts add constraint uniq_rewards_gold_12_operator_od_operator_set_reward_amounts unique (reward_hash, operator_set_id, operator, strategy, snapshot);`,
			},
			NewTableName:         "rewards_gold_12_operator_od_operator_set_reward_amounts",
			ExistingTablePattern: "gold_[0-9]+_operator_od_operator_set_reward_amounts_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_13_staker_od_operator_set_reward_amounts (
				reward_hash                            varchar,
				snapshot                               date,
				token                                  varchar,
				tokens_per_registered_snapshot_decimal numeric,
				avs                                    varchar,
				operator_set_id                        bigint,
				operator                               varchar,
				strategy                               varchar,
				multiplier                             numeric(78),
				reward_submission_date                 text,
				staker_split                           numeric,
				staker                                 varchar,
				shares                                 numeric,
				staker_weight                          numeric,
				rn                                     bigint,
				total_weight                           numeric,
				staker_proportion                      numeric,
				staker_tokens                          numeric,
    			generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_13_staker_od_operator_set_reward_amounts add constraint uniq_rewards_gold_13_staker_od_operator_set_reward_amounts unique (reward_hash, snapshot, operator_set_id, operator, strategy);`,
			},
			NewTableName:         "rewards_gold_13_staker_od_operator_set_reward_amounts",
			ExistingTablePattern: "gold_[0-9]+_staker_od_operator_set_reward_amounts_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_14_avs_od_operator_set_reward_amounts (
				reward_hash     varchar,
				snapshot        date,
				token           varchar,
				avs             varchar,
				operator_set_id bigint,
				operator        varchar,
				avs_tokens      numeric,
				generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_14_avs_od_operator_set_reward_amounts add constraint uniq_rewards_gold_14_avs_od_operator_set_reward_amounts unique (reward_hash, snapshot, operator_set_id, operator, token);`,
			},
			NewTableName:         "rewards_gold_14_avs_od_operator_set_reward_amounts",
			ExistingTablePattern: "gold_[0-9]+_avs_od_operator_set_reward_amounts_[0-9_]+$",
		},
		{
			CreateTableQuery: `create table if not exists rewards_gold_staging (
				earner      varchar,
				snapshot    date,
				reward_hash varchar,
				token       varchar,
				amount      numeric,
				generated_rewards_snapshot_id bigint,
    			foreign key (generated_rewards_snapshot_id) references generated_rewards_snapshots (id) on delete cascade
			);`,
			PreDataMigrationQueries: []string{
				`alter table rewards_gold_staging add constraint uniq_rewards_gold_staging unique (earner, token, reward_hash, snapshot);`,
			},
			NewTableName:         "rewards_gold_staging",
			ExistingTablePattern: "gold_[0-9]+_gold_staging_[0-9_]+$",
		},
	}

	_, err := helpers.WrapTxAndCommit(func(tx *gorm.DB) (interface{}, error) {
		for _, sm := range subMigrations {
			fmt.Printf("Running migration for table: %s\n", sm.NewTableName)
			if err := sm.Run(db, tx, cfg); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}, grm, nil)
	if err != nil {
		return err
	}

	return nil
}

func (m *Migration) GetName() string {
	return "202505081218_migrateRewardsTables"
}
