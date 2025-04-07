package _202503030846_cleanupConstraintNames

import (
	"database/sql"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/base"
	"gorm.io/gorm"
)

type Migration struct {
}

func formatRenameQuery(tableName, constraintName string) string {
	newName := base.FormatUniqueConstraintName(tableName)

	return fmt.Sprintf("ALTER TABLE %s RENAME CONSTRAINT %s TO %s", tableName, constraintName, newName)
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		formatRenameQuery("avs_operator_state_changes", "avs_operator_state_changes_operator_avs_block_number_log_in_key"),
		formatRenameQuery("disabled_distribution_roots", "uniq_disabled_distribution_root"),
		formatRenameQuery("default_operator_splits", "default_operator_splits_transaction_hash_log_index_block_nu_key"),
		formatRenameQuery("operator_avs_splits", "operator_avs_splits_transaction_hash_log_index_block_number_key"),
		formatRenameQuery("operator_directed_operator_set_reward_submissions", "operator_directed_operator_se_transaction_hash_log_index_bl_key"),
		formatRenameQuery("operator_directed_reward_submissions", "operator_directed_reward_subm_transaction_hash_log_index_bl_key"),
		formatRenameQuery("operator_pi_splits", "operator_pi_splits_transaction_hash_log_index_block_number_key"),
		formatRenameQuery("operator_set_operator_registrations", "operator_set_operator_registr_transaction_hash_log_index_bl_key"),
		formatRenameQuery("operator_set_splits", "operator_set_splits_transaction_hash_log_index_block_number_key"),
		formatRenameQuery("operator_set_strategy_registrations", "operator_set_strategy_registr_transaction_hash_log_index_bl_key"),
		formatRenameQuery("reward_submissions", "uniq_reward_submission"),
		formatRenameQuery("staker_delegation_changes", "uniq_staker_delegation_change"),
		formatRenameQuery("submitted_distribution_roots", "uniq_submitted_distribution_root"),
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
	return "202503030846_cleanupConstraintNames"
}
