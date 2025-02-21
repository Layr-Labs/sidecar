package _202502252204_operatorSplitModel

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS operator_sets (
			operator_set_id bigint not null,
			avs varchar not null,
			block_number bigint not null,
			transaction_hash varchar not null,
			log_index bigint not null,
			unique (transaction_hash, log_index, block_number)
    	)`,
		`create index if not exists idx_operator_sets_avs on operator_sets (avs)`,
		`create index if not exists idx_operator_sets_block_number on operator_sets (block_number)`,
	}
	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			return res.Error
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202502252204_operatorSplitModel"
}
