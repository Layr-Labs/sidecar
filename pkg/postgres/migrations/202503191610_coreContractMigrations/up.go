package _202503191610_coreContractMigrations

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	query := `create table if not exists core_contract_migrations(
    			id serial primary key,
    			name text not null,
    			metadata jsonb default '{}',
    			created_at timestamp default current_timestamp
	)`

	res := grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202503191610_coreContractMigrations"
}
