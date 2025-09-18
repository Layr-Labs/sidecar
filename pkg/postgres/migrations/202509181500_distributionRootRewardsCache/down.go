package _202509181500_distributionRootRewardsCache

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

func (m *Migration) Down(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	// Drop the cache table and its indexes
	queries := []string{
		`DROP INDEX IF EXISTS idx_distribution_root_rewards_cache_root_index;`,
		`DROP TABLE IF EXISTS distribution_root_rewards_cache;`,
	}

	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			return res.Error
		}
	}
	return nil
}
