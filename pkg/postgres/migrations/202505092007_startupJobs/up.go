package _202505092007_startupJobs

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	query := `
	create table if not exists startup_jobs (
		id serial primary key,
		name text not null,
	    created_at timestamp with time zone default current_timestamp,
	    unique(name)
	)`

	res := grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202505092007_startupJobs"
}
