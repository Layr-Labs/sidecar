package _202505081218_migrateRewardsTables

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

func getMinGeneratedRewardsSnapshotId(grm *gorm.DB) (uint64, error) {
	var minSnapshotId *uint64
	res := grm.Raw("select min(id) from generated_rewards_snapshots").Scan(&minSnapshotId)
	if res.Error != nil {
		return 0, res.Error
	}
	if minSnapshotId == nil {
		return 0, nil // Return 0 if no snapshots exist
	}
	return *minSnapshotId, nil
}

func (sm *SubMigration) Run(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	fmt.Printf("Creating table: %s\n", sm.NewTableName)
	res := grm.Exec(sm.CreateTableQuery)
	if res.Error != nil {
		fmt.Printf("Failed to create table %s: %s\nQuery: %s\n", sm.NewTableName, res.Error, sm.CreateTableQuery)
		return res.Error
	}

	fmt.Printf("Adding constraints for table: %s\n", sm.NewTableName)
	for _, query := range sm.PreDataMigrationQueries {
		res := grm.Exec(query)
		if res.Error != nil {
			fmt.Printf("Failed to add constraints for table %s: %s\nQuery: %s\n", sm.NewTableName, res.Error, query)
			return res.Error
		}
	}

	fmt.Printf("Searching for existing tables with pattern: %s\n", sm.ExistingTablePattern)
	likeTablesQuery := `
		SELECT table_schema || '.' || table_name
		FROM information_schema.tables
		WHERE table_type='BASE TABLE'
			and table_name ~* @pattern
	`
	var tables []string
	res = grm.Raw(likeTablesQuery, sql.Named("pattern", sm.ExistingTablePattern)).Scan(&tables)
	if res.Error != nil {
		fmt.Printf("Failed to find tables: %s\n%s\n", sm.ExistingTablePattern, likeTablesQuery)
		return res.Error
	}
	fmt.Printf("Found %d existing tables to migrate: %v\n", len(tables), tables)

	if len(tables) == 0 {
		fmt.Printf("No tables found to migrate for pattern: %s\n", sm.ExistingTablePattern)
		return nil
	}

	// Optimize PostgreSQL settings for bulk inserts (only settings that can be changed at runtime)
	fmt.Printf("Optimizing PostgreSQL settings for bulk migration\n")
	optimizationQueries := []string{
		"SET work_mem = '256MB'",    // Increase memory for sort operations - can be changed at runtime
		"SET temp_buffers = '32MB'", // Increase temporary buffer size - can be changed at runtime
	}

	for _, query := range optimizationQueries {
		res := grm.Exec(query)
		if res.Error != nil {
			fmt.Printf("Warning: Failed to set optimization setting: %s (error: %s)\n", query, res.Error)
			// Continue anyway, these are just optimizations
		} else {
			fmt.Printf("Successfully set: %s\n", query)
		}
	}

	// Process tables in smaller batches for better performance and reduced memory usage
	batchSize := 10 // Process 10 tables at a time
	for i := 0; i < len(tables); i += batchSize {
		end := i + batchSize
		if end > len(tables) {
			end = len(tables)
		}

		fmt.Printf("Processing batch %d-%d of %d tables\n", i+1, end, len(tables))

		for j := i; j < end; j++ {
			table := tables[j]
			if err := sm.processSingleTable(table, grm); err != nil {
				return err
			}
		}

		fmt.Printf("Completed batch %d-%d\n", i+1, end)
	}

	fmt.Printf("Migration completed for table: %s\n", sm.NewTableName)
	return nil
}

func (sm *SubMigration) processSingleTable(table string, grm *gorm.DB) error {
	fmt.Printf("Processing table: %s\n", table)

	// find the corresponding generated rewards entry
	// Try multiple regex patterns for different date formats
	patterns := []string{
		`(202[0-9]_[0-9]{2}_[0-9]{2})$`,     // 2024_12_31
		`(202[0-9]_[0-9]_[0-9]{2})$`,        // 2024_1_31
		`(202[0-9]_[0-9]{2}_[0-9])$`,        // 2024_12_1
		`(202[0-9]_[0-9]_[0-9])$`,           // 2024_1_1
		`([0-9]{4}_[0-9]{1,2}_[0-9]{1,2})$`, // Generic YYYY_M_D or YYYY_MM_DD
	}

	var match []string
	date := ""
	for _, pattern := range patterns {
		re := regexp.MustCompile(pattern)
		match = re.FindStringSubmatch(table)
		if len(match) == 2 {
			date = match[1]
			break
		}
	}

	if len(match) != 2 {
		fmt.Printf("Failed to find date in table name: %s, tried patterns: %v\n", table, patterns)
		return fmt.Errorf("Failed to find date in table name: %s", table)
	}
	// date := match[1]
	kebabDate := strings.ReplaceAll(date, "_", "-")
	fmt.Printf("Extracted date: %s -> %s from table: %s\n", date, kebabDate, table)

	generatedQuery := `select id from generated_rewards_snapshots where snapshot_date = @snapshot`
	var snapshotId uint64
	res := grm.Raw(generatedQuery, sql.Named("snapshot", kebabDate)).Scan(&snapshotId)
	if res.Error != nil && !errors.Is(res.Error, gorm.ErrRecordNotFound) {
		fmt.Printf("Failed to find generated rewards snapshot for date: %s\n", date)
		return res.Error
	}
	if errors.Is(res.Error, gorm.ErrRecordNotFound) {
		fmt.Printf("No generated rewards snapshot found for date: %s, getting min snapshot ID\n", date)
		var err error
		snapshotId, err = getMinGeneratedRewardsSnapshotId(grm)
		if err != nil {
			fmt.Printf("Failed to get min snapshot ID: %v\n", err)
			snapshotId = 0
		}
		fmt.Printf("Using min snapshot ID: %d\n", snapshotId)
	} else {
		fmt.Printf("Found snapshot ID %d for date: %s\n", snapshotId, kebabDate)
	}

	fmt.Printf("Inserting data from %s to %s\n", table, sm.NewTableName)

	// Optimize: Include snapshot_id directly in the INSERT query instead of separate UPDATE
	var query string
	if snapshotId == 0 {
		// If no snapshot ID, insert without it (will be NULL)
		query = fmt.Sprintf("insert into %s select * from %s on conflict on constraint uniq_%s do nothing", sm.NewTableName, table, sm.NewTableName)
	} else {
		// Get column list for the destination table (excluding generated_rewards_snapshot_id)
		var columns []string
		columnQuery := `
			SELECT column_name 
			FROM information_schema.columns 
			WHERE table_name = @table_name 
			AND column_name != 'generated_rewards_snapshot_id'
			ORDER BY ordinal_position
		`
		res := grm.Raw(columnQuery, sql.Named("table_name", sm.NewTableName)).Scan(&columns)
		if res.Error != nil {
			fmt.Printf("Failed to get columns for table %s: %s\n", sm.NewTableName, res.Error)
			return res.Error
		}

		columnList := strings.Join(columns, ", ")
		// Include snapshot_id directly in the INSERT
		query = fmt.Sprintf(
			"insert into %s (%s, generated_rewards_snapshot_id) select %s, %d from %s on conflict on constraint uniq_%s do nothing",
			sm.NewTableName, columnList, columnList, snapshotId, table, sm.NewTableName,
		)
	}

	res = grm.Exec(query)
	if res.Error != nil {
		fmt.Printf("Failed to insert data from %s: %s\nQuery: %s\n", table, res.Error, query)
		return res.Error
	}
	fmt.Printf("Successfully inserted data from %s (affected rows: %d)\n", table, res.RowsAffected)
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
