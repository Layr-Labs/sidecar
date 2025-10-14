package _202508221218_migrateRewardsTables

import (
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/postgres/helpers"
	"gorm.io/gorm"
)

type Migration struct {
}

type SubMigration struct {
	CreateTableQuery        string
	NewTableName            string
	ExistingTablePattern    string
	PreDataMigrationQueries []string
}

func (sm *SubMigration) Run(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	// Create the new table
	res := grm.Exec(sm.CreateTableQuery)
	if res.Error != nil {
		fmt.Printf("Failed to execute query: %s\n", sm.CreateTableQuery)
		return res.Error
	}

	// Execute pre-data migration queries
	for _, query := range sm.PreDataMigrationQueries {
		res := grm.Exec(query)
		if res.Error != nil {
			fmt.Printf("Failed to execute query: %s\n", query)
			return res.Error
		}
	}

	// Find matching tables
	likeTablesQuery := `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_type='BASE TABLE'
			and table_name ~* @pattern
			and table_schema = 'public'
	`
	var tables []string
	res = grm.Raw(likeTablesQuery, sql.Named("pattern", sm.ExistingTablePattern)).Scan(&tables)
	if res.Error != nil {
		fmt.Printf("Failed to find tables: %s\n%s\n", sm.ExistingTablePattern, likeTablesQuery)
		return res.Error
	}

	for _, table := range tables {
		// Extract date from table name
		re := regexp.MustCompile(`(202[0-9]_[0-9]{2}_[0-9]{2})$`)
		match := re.FindStringSubmatch(table)
		if len(match) != 2 {
			return fmt.Errorf("failed to find date in table name: %s", table)
		}
		date := match[1]
		kebabDate := strings.ReplaceAll(date, "_", "-")

		// Find corresponding generated rewards snapshot (STRICT - fail if not found)
		generatedQuery := `select id from generated_rewards_snapshots where snapshot_date = @snapshot`
		var snapshotId uint64
		res = grm.Raw(generatedQuery, sql.Named("snapshot", kebabDate)).Scan(&snapshotId)
		if res.Error != nil {
			if errors.Is(res.Error, gorm.ErrRecordNotFound) {
				return fmt.Errorf("CRITICAL: No generated rewards snapshot found for date: %s. Migration cannot proceed safely - this would corrupt data. Please ensure snapshot exists before migrating", kebabDate)
			}
			return fmt.Errorf("failed to query generated rewards snapshot for date %s: %w", kebabDate, res.Error)
		}
		if snapshotId == 0 {
			return fmt.Errorf("CRITICAL: Invalid snapshot ID (0) found for date: %s. Migration cannot proceed safely", kebabDate)
		}

		// Migrate data in batches with proper conflict resolution
		insertBatchSize := 300000
		offset := 0
		var tableMigratedRecords int64

		for {
			// Insert directly from source table to final table with generated_rewards_snapshot_id
			migrateQuery := fmt.Sprintf(`
				INSERT INTO %s 
				SELECT *, %d as generated_rewards_snapshot_id
				FROM %s 
				ORDER BY ctid 
				LIMIT %d OFFSET %d
				ON CONFLICT ON CONSTRAINT uniq_%s 
				DO NOTHING
			`, sm.NewTableName, snapshotId, table, insertBatchSize, offset, sm.NewTableName)

			res := grm.Exec(migrateQuery)
			if res.Error != nil {
				return fmt.Errorf("Migration failed for table %s at offset %d: %w", table, offset, res.Error)
			}

			rowsAffected := res.RowsAffected
			if rowsAffected == 0 {
				break
			}

			tableMigratedRecords += rowsAffected
			fmt.Printf("Migrated %d records from %s (offset %d)\n", rowsAffected, table, offset)

			// If we got fewer rows than batch size, we're done
			if rowsAffected < int64(insertBatchSize) {
				break
			}

			offset += insertBatchSize
		}

		// Drop the source table
		dropQuery := fmt.Sprintf("DROP TABLE IF EXISTS %s", table)
		res = grm.Exec(dropQuery)
		if res.Error != nil {
			fmt.Printf("Failed to drop table %s: %s\n", table, res.Error)
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_1_active_rewards') THEN
						ALTER TABLE rewards_gold_1_active_rewards ADD CONSTRAINT uniq_rewards_gold_1_active_rewards UNIQUE (avs, reward_hash, strategy, snapshot);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_2_staker_reward_amounts') THEN
						ALTER TABLE rewards_gold_2_staker_reward_amounts ADD CONSTRAINT uniq_rewards_gold_2_staker_reward_amounts UNIQUE (reward_hash, staker, avs, strategy, snapshot);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_3_operator_reward_amounts') THEN
						ALTER TABLE rewards_gold_3_operator_reward_amounts ADD CONSTRAINT uniq_rewards_gold_3_operator_reward_amounts UNIQUE (reward_hash, avs, operator, strategy, snapshot);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_4_rewards_for_all') THEN
						ALTER TABLE rewards_gold_4_rewards_for_all ADD CONSTRAINT uniq_rewards_gold_4_rewards_for_all UNIQUE (reward_hash, avs, staker, strategy, snapshot);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_5_rfae_stakers') THEN
						ALTER TABLE rewards_gold_5_rfae_stakers ADD CONSTRAINT uniq_rewards_gold_5_rfae_stakers UNIQUE (reward_hash, avs, staker, operator, strategy, snapshot);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_6_rfae_operators') THEN
						ALTER TABLE rewards_gold_6_rfae_operators ADD CONSTRAINT uniq_rewards_gold_6_rfae_operators UNIQUE (reward_hash, avs, operator, strategy, snapshot);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_7_active_od_rewards') THEN
						ALTER TABLE rewards_gold_7_active_od_rewards ADD CONSTRAINT uniq_rewards_gold_7_active_od_rewards UNIQUE (reward_hash, avs, operator, strategy, snapshot);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_8_operator_od_reward_amounts') THEN
						ALTER TABLE rewards_gold_8_operator_od_reward_amounts ADD CONSTRAINT uniq_rewards_gold_8_operator_od_reward_amounts UNIQUE (reward_hash, avs, operator, strategy, snapshot);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_9_staker_od_reward_amounts') THEN
						ALTER TABLE rewards_gold_9_staker_od_reward_amounts ADD CONSTRAINT uniq_rewards_gold_9_staker_od_reward_amounts UNIQUE (reward_hash, avs, operator, staker, strategy, snapshot);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_10_avs_od_reward_amounts') THEN
						ALTER TABLE rewards_gold_10_avs_od_reward_amounts ADD CONSTRAINT uniq_rewards_gold_10_avs_od_reward_amounts UNIQUE (reward_hash, avs, operator, snapshot);
					END IF;
				END $$;`,
			},
			NewTableName:         "rewards_gold_10_avs_od_reward_amounts",
			ExistingTablePattern: "gold_[0-9]+_avs_od_reward_amounts_[0-9_]+$",
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_11_active_od_operator_set_rewards') THEN
						ALTER TABLE rewards_gold_11_active_od_operator_set_rewards ADD CONSTRAINT uniq_rewards_gold_11_active_od_operator_set_rewards UNIQUE (reward_hash, operator_set_id, strategy, snapshot);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_12_operator_od_operator_set_reward_amounts') THEN
						ALTER TABLE rewards_gold_12_operator_od_operator_set_reward_amounts ADD CONSTRAINT uniq_rewards_gold_12_operator_od_operator_set_reward_amounts UNIQUE (reward_hash, operator_set_id, operator, strategy, snapshot);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_13_staker_od_operator_set_reward_amounts') THEN
						ALTER TABLE rewards_gold_13_staker_od_operator_set_reward_amounts ADD CONSTRAINT uniq_rewards_gold_13_staker_od_operator_set_reward_amounts UNIQUE (reward_hash, snapshot, operator_set_id, operator, strategy);
					END IF;
				END $$;`,
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
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_rewards_gold_14_avs_od_operator_set_reward_amounts') THEN
						ALTER TABLE rewards_gold_14_avs_od_operator_set_reward_amounts ADD CONSTRAINT uniq_rewards_gold_14_avs_od_operator_set_reward_amounts UNIQUE (reward_hash, snapshot, operator_set_id, operator, token);
					END IF;
				END $$;`,
			},
			NewTableName:         "rewards_gold_14_avs_od_operator_set_reward_amounts",
			ExistingTablePattern: "gold_[0-9]+_avs_od_operator_set_reward_amounts_[0-9_]+$",
		},
		{
			CreateTableQuery: `
				DROP TABLE IF EXISTS gold_table;
				CREATE TABLE gold_table (
					earner      varchar,
					snapshot    date,
					reward_hash varchar,
					token       varchar,
					amount      numeric,
					generated_rewards_snapshot_id bigint,
					FOREIGN KEY (generated_rewards_snapshot_id) REFERENCES generated_rewards_snapshots (id) ON DELETE CASCADE
				);`,
			PreDataMigrationQueries: []string{
				`DO $$ 
				BEGIN
					IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uniq_gold_table') THEN
						ALTER TABLE gold_table ADD CONSTRAINT uniq_gold_table UNIQUE (earner, token, reward_hash, snapshot);
					END IF;
				END $$;`,
			},
			NewTableName:         "gold_table",
			ExistingTablePattern: "gold_[0-9]+_staging_[0-9_]+$",
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
	return "202505301218_migrateRewardsTables"
}
