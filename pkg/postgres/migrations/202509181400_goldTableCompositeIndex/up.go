package _202509181400_goldTableCompositeIndex

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	// Create composite index optimized for the FetchRewardsForSnapshot query
	query := `create index concurrently if not exists idx_gold_table_composite on gold_table (snapshot, reward_hash, earner, token);`

	res := grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202509181400_goldTableCompositeIndex"
}
