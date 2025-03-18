package _202503181244_snapshotUniqueConstraintsPartTwo

import (
	"database/sql"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`truncate table operator_directed_operator_set_rewards cascade`,
		`alter table operator_directed_operator_set_rewards add constraint uniq_operator_directed_operator_set_rewards unique (block_number, reward_hash, strategy_index, operator_index)`,

		`truncate table operator_set_operator_registration_snapshots cascade`,
		`alter table operator_set_operator_registration_snapshots add constraint uniq_operator_set_operator_registration_snapshots unique (avs, operator, operator_set_id, snapshot)`,

		`truncate table operator_set_split_snapshots cascade`,
		`alter table operator_set_split_snapshots add constraint uniq_operator_set_split_snapshots unique (operator, avs, operator_set_id, snapshot)`,

		`truncate table operator_set_strategy_registration_snapshots cascade`,
		`alter table operator_set_strategy_registration_snapshots add constraint uniq_operator_set_strategy_registration_snapshots unique (avs, operator_set_id, strategy, snapshot)`,
	}

	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			fmt.Printf("Error executing query: %s\n", query)
			return res.Error
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202503181244_snapshotUniqueConstraintsPartTwo"
}
