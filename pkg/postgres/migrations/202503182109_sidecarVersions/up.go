package _202503182109_sidecarVersions

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {

	query := `
		create table if not exists sidecar_versions (
			id serial primary key,
			version text not null,
			state_root_block_launched_at bigint not null,
			created_at timestamp with time zone default current_timestamp
		)
	`
	res := grm.Exec(query)
	return res.Error
}

func (m *Migration) GetName() string {
	return "202503182109_sidecarVersions"
}
