package _202509181500_distributionRootRewardsCache

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	// Create cache table for pre-computed distribution root rewards
	query := `
		CREATE TABLE IF NOT EXISTS distribution_root_rewards_cache (
			root_index bigint NOT NULL,
			earner varchar NOT NULL,
			token varchar NOT NULL,
			snapshot date NOT NULL,
			cumulative_amount numeric NOT NULL,
			created_at timestamp DEFAULT now(),
			PRIMARY KEY (root_index, earner, token)
		);
		
		-- Index for efficient lookups
		CREATE INDEX IF NOT EXISTS idx_distribution_root_rewards_cache_root_index 
		ON distribution_root_rewards_cache (root_index);
	`

	res := grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202509181500_distributionRootRewardsCache"
}
