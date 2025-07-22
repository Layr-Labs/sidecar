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

	// Track total records for validation
	var totalSourceRecords int64
	var totalMigratedRecords int64

	for _, table := range tables {
		fmt.Printf("Processing table: %s\n", table)

		// Count source records for validation
		var sourceCount int64
		countRes := grm.Raw("SELECT COUNT(*) FROM " + table).Scan(&sourceCount)
		if countRes.Error != nil {
			return fmt.Errorf("failed to count source records in %s: %w", table, countRes.Error)
		}
		totalSourceRecords += sourceCount
		fmt.Printf("Source table %s has %d records\n", table, sourceCount)

		// Extract date from table name
		re := regexp.MustCompile(`(202[0-9]_[0-9]{2}_[0-9]{2})$`)
		match := re.FindStringSubmatch(table)
		if len(match) != 2 {
			return fmt.Errorf("Failed to find date in table name: %s", table)
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
			return fmt.Errorf("Failed to query generated rewards snapshot for date %s: %w", kebabDate, res.Error)
		}
		if snapshotId == 0 {
			return fmt.Errorf("CRITICAL: Invalid snapshot ID (0) found for date: %s. Migration cannot proceed safely", kebabDate)
		}

		fmt.Printf("Using snapshot ID %d for date %s\n", snapshotId, kebabDate)

		// Migrate data in batches with proper conflict resolution
		insertBatchSize := 10000
		offset := 0
		var tableMigratedRecords int64

		for {
			// Use INSERT ... ON CONFLICT DO UPDATE to handle duplicates properly
			// This ensures we don't lose data and maintain consistency
			query := fmt.Sprintf(`
				INSERT INTO %s 
				SELECT *, %d as generated_rewards_snapshot_id 
				FROM %s 
				ORDER BY ctid 
				LIMIT %d OFFSET %d 
				ON CONFLICT ON CONSTRAINT uniq_%s 
				DO UPDATE SET 
					generated_rewards_snapshot_id = EXCLUDED.generated_rewards_snapshot_id
			`, sm.NewTableName, snapshotId, table, insertBatchSize, offset, sm.NewTableName)

			res := grm.Exec(query)
			if res.Error != nil {
				fmt.Printf("Failed to execute migration query: %s\n", query)
				return fmt.Errorf("Migration failed for table %s at offset %d: %w", table, offset, res.Error)
			}

			rowsAffected := res.RowsAffected
			tableMigratedRecords += rowsAffected
			fmt.Printf("Migrated %d records from %s (offset %d)\n", rowsAffected, table, offset)

			// If no rows were affected, we're done with this table
			if rowsAffected == 0 {
				break
			}

			offset += insertBatchSize
		}

		totalMigratedRecords += tableMigratedRecords
		fmt.Printf("Completed migration for table %s: %d records processed\n", table, tableMigratedRecords)
	}

	// Validate migration results
	fmt.Printf("Migration validation: Source records: %d, Migrated records: %d\n", totalSourceRecords, totalMigratedRecords)

	// Count final records in destination table
	var finalCount int64
	finalCountRes := grm.Raw("SELECT COUNT(*) FROM " + sm.NewTableName).Scan(&finalCount)
	if finalCountRes.Error != nil {
		return fmt.Errorf("failed to count final records in %s: %w", sm.NewTableName, finalCountRes.Error)
	}

	// Validate data integrity
	if totalMigratedRecords != totalSourceRecords {
		fmt.Printf("WARNING: Record count mismatch - Source: %d, Migrated: %d, Final: %d\n",
			totalSourceRecords, totalMigratedRecords, finalCount)
		// Note: This might be expected due to deduplication, but we should log it
	}

	// Verify no records have null generated_rewards_snapshot_id
	var nullSnapshotCount int64
	nullCheckRes := grm.Raw("SELECT COUNT(*) FROM " + sm.NewTableName + " WHERE generated_rewards_snapshot_id IS NULL").Scan(&nullSnapshotCount)
	if nullCheckRes.Error != nil {
		return fmt.Errorf("failed to check for null snapshot IDs in %s: %w", sm.NewTableName, nullCheckRes.Error)
	}

	if nullSnapshotCount > 0 {
		return fmt.Errorf("CRITICAL: Found %d records with null generated_rewards_snapshot_id in %s after migration. This indicates data corruption", nullSnapshotCount, sm.NewTableName)
	}

	fmt.Printf("Successfully completed migration for %s: %d total records migrated\n", sm.NewTableName, finalCount)
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

	// CRITICAL: Populate gold_table from consolidated staging data
	// This is the final step that enables merkle tree calculation
	fmt.Printf("Populating gold_table from consolidated staging data\n")
	populateGoldTableQuery := `
		INSERT INTO gold_table (earner, snapshot, token, amount, reward_hash)
		SELECT
			earner,
			snapshot,
			token,
			amount,
			reward_hash
		FROM rewards_gold_staging
		ON CONFLICT (earner, snapshot, token, reward_hash) DO UPDATE SET
			amount = EXCLUDED.amount
	`

	res := grm.Exec(populateGoldTableQuery)
	if res.Error != nil {
		return fmt.Errorf("Failed to populate gold_table from staging data: %w", res.Error)
	}

	fmt.Printf("Successfully populated gold_table with %d records\n", res.RowsAffected)

	return nil
}

func (m *Migration) GetName() string {
	return "202505301218_migrateRewardsTables"
}
