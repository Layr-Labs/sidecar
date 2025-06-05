package _202506042023_addMissingBlockNumberForeignKeys

import (
	"database/sql"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	tableNames := []string{
		"staker_shares",
		"operator_shares",
	}

	for _, tableName := range tableNames {
		query := fmt.Sprintf(`alter table %s add constraint %s_block_number_fkey foreign key (block_number) references blocks (number) on delete cascade;`, tableName, tableName)
		res := grm.Exec(query)
		if res.Error != nil {
			return fmt.Errorf("failed to add foreign key for table %s: %w", tableName, res.Error)
		}
	}

	queries := []string{
		`create index if not exists idx_staker_shares_block_number on sidecar_mainnet_ethereum.staker_shares(block_number);`,
		`create index if not exists idx_operator_shares_block_number on sidecar_mainnet_ethereum.operator_shares(block_number);`,
	}
	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			return fmt.Errorf("failed to create index: %w", res.Error)
		}
	}

	return nil
}

func (m *Migration) GetName() string {
	return "202506042023_addMissingBlockNumberForeignKeys"
}
