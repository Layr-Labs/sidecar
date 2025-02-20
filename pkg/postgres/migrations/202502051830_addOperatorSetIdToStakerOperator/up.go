package _202502051830_addOperatorSetIdToStakerOperator

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`alter table staker_operator add column operator_set_id bigint`,
		`alter table staker_operator drop constraint uniq_staker_operator`,
		`alter table staker_operator add constraint uniq_staker_operator unique (earner, operator, snapshot, reward_hash, strategy, reward_type, avs, operator_set_id)`,
	}

	for _, query := range queries {
		if err := grm.Exec(query).Error; err != nil {
			return err
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202502051830_addOperatorSetIdToStakerOperator"
}
