package rewardsUtils

import (
	"bytes"
	"database/sql"
	"fmt"
	"text/template"

	"github.com/Layr-Labs/sidecar/pkg/postgres/helpers"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var (
	Table_1_ActiveRewards                       = "gold_1_active_rewards"
	Table_2_StakerRewardAmounts                 = "gold_2_staker_reward_amounts"
	Table_3_OperatorRewardAmounts               = "gold_3_operator_reward_amounts"
	Table_4_RewardsForAll                       = "gold_4_rewards_for_all"
	Table_5_RfaeStakers                         = "gold_5_rfae_stakers"
	Table_6_RfaeOperators                       = "gold_6_rfae_operators"
	Table_7_ActiveODRewards                     = "gold_7_active_od_rewards"
	Table_8_OperatorODRewardAmounts             = "gold_8_operator_od_reward_amounts"
	Table_9_StakerODRewardAmounts               = "gold_9_staker_od_reward_amounts"
	Table_10_AvsODRewardAmounts                 = "gold_10_avs_od_reward_amounts"
	Table_11_ActiveODOperatorSetRewards         = "gold_11_active_od_operator_set_rewards"
	Table_12_OperatorODOperatorSetRewardAmounts = "gold_12_operator_od_operator_set_reward_amounts"
	Table_13_StakerODOperatorSetRewardAmounts   = "gold_13_staker_od_operator_set_reward_amounts"
	Table_14_AvsODOperatorSetRewardAmounts      = "gold_14_avs_od_operator_set_reward_amounts"
	Table_15_GoldStaging                        = "gold_15_staging"
	Table_16_GoldTable                          = "gold_table"

	Sot_1_StakerStrategyPayouts       = "sot_1_staker_strategy_payouts"
	Sot_2_OperatorStrategyPayouts     = "sot_2_operator_strategy_payouts"
	Sot_3_RewardsForAllStrategyPayout = "sot_3_rewards_for_all_strategy_payout"
	Sot_4_RfaeStakers                 = "sot_4_rfae_stakers"
	Sot_5_RfaeOperators               = "sot_5_rfae_operators"
	Sot_6_OperatorODStrategyPayouts   = "sot_6_operator_od_strategy_payouts"
	Sot_7_StakerODStrategyPayouts     = "sot_7_staker_od_strategy_payouts"
	Sot_8_AvsODStrategyPayouts        = "sot_8_avs_od_strategy_payouts"
	Sot_9_StakerOperatorStaging       = "sot_9_staker_operator_staging"
	Sot_10_StakerOperatorTable        = "staker_operator"
)

var goldTableBaseNames = map[string]string{
	Table_1_ActiveRewards:                       Table_1_ActiveRewards,
	Table_2_StakerRewardAmounts:                 Table_2_StakerRewardAmounts,
	Table_3_OperatorRewardAmounts:               Table_3_OperatorRewardAmounts,
	Table_4_RewardsForAll:                       Table_4_RewardsForAll,
	Table_5_RfaeStakers:                         Table_5_RfaeStakers,
	Table_6_RfaeOperators:                       Table_6_RfaeOperators,
	Table_7_ActiveODRewards:                     Table_7_ActiveODRewards,
	Table_8_OperatorODRewardAmounts:             Table_8_OperatorODRewardAmounts,
	Table_9_StakerODRewardAmounts:               Table_9_StakerODRewardAmounts,
	Table_10_AvsODRewardAmounts:                 Table_10_AvsODRewardAmounts,
	Table_11_ActiveODOperatorSetRewards:         Table_11_ActiveODOperatorSetRewards,
	Table_12_OperatorODOperatorSetRewardAmounts: Table_12_OperatorODOperatorSetRewardAmounts,
	Table_13_StakerODOperatorSetRewardAmounts:   Table_13_StakerODOperatorSetRewardAmounts,
	Table_14_AvsODOperatorSetRewardAmounts:      Table_14_AvsODOperatorSetRewardAmounts,
	Table_15_GoldStaging:                        Table_15_GoldStaging,
	Table_16_GoldTable:                          Table_16_GoldTable,

	Sot_1_StakerStrategyPayouts:       Sot_1_StakerStrategyPayouts,
	Sot_2_OperatorStrategyPayouts:     Sot_2_OperatorStrategyPayouts,
	Sot_3_RewardsForAllStrategyPayout: Sot_3_RewardsForAllStrategyPayout,
	Sot_4_RfaeStakers:                 Sot_4_RfaeStakers,
	Sot_5_RfaeOperators:               Sot_5_RfaeOperators,
	Sot_6_OperatorODStrategyPayouts:   Sot_6_OperatorODStrategyPayouts,
	Sot_7_StakerODStrategyPayouts:     Sot_7_StakerODStrategyPayouts,
	Sot_8_AvsODStrategyPayouts:        Sot_8_AvsODStrategyPayouts,
	Sot_9_StakerOperatorStaging:       Sot_9_StakerOperatorStaging,
}

var GoldTableNameSearchPattern = map[string]string{
	Table_1_ActiveRewards:                       "gold_%_active_rewards",
	Table_2_StakerRewardAmounts:                 "gold_%_staker_reward_amounts",
	Table_3_OperatorRewardAmounts:               "gold_%_operator_reward_amounts",
	Table_4_RewardsForAll:                       "gold_%_rewards_for_all",
	Table_5_RfaeStakers:                         "gold_%_rfae_stakers",
	Table_6_RfaeOperators:                       "gold_%_rfae_operators",
	Table_7_ActiveODRewards:                     "gold_%_active_od_rewards",
	Table_8_OperatorODRewardAmounts:             "gold_%_operator_od_reward_amounts",
	Table_9_StakerODRewardAmounts:               "gold_%_staker_od_reward_amounts",
	Table_10_AvsODRewardAmounts:                 "gold_%_avs_od_reward_amounts",
	Table_11_ActiveODOperatorSetRewards:         "gold_%_active_od_operator_set_rewards",
	Table_12_OperatorODOperatorSetRewardAmounts: "gold_%_operator_od_operator_set_reward_amounts",
	Table_13_StakerODOperatorSetRewardAmounts:   "gold_%_staker_od_operator_set_reward_amounts",
	Table_14_AvsODOperatorSetRewardAmounts:      "gold_%_avs_od_operator_set_reward_amounts",
	Table_15_GoldStaging:                        "gold_%_staging",

	Sot_1_StakerStrategyPayouts:       "sot_%_staker_strategy_payouts",
	Sot_2_OperatorStrategyPayouts:     "sot_%_operator_strategy_payouts",
	Sot_3_RewardsForAllStrategyPayout: "sot_%_rewards_for_all_strategy_payout",
	Sot_4_RfaeStakers:                 "sot_%_rfae_stakers",
	Sot_5_RfaeOperators:               "sot_%_rfae_operators",
	Sot_6_OperatorODStrategyPayouts:   "sot_%_operator_od_strategy_payouts",
	Sot_7_StakerODStrategyPayouts:     "sot_%_staker_od_strategy_payouts",
	Sot_8_AvsODStrategyPayouts:        "sot_%_avs_od_strategy_payouts",
	Sot_9_StakerOperatorStaging:       "sot_%_staker_operator_staging",
}

func GetGoldTableNames(snapshotDate string) map[string]string {
	tableNames := make(map[string]string)
	for key, baseName := range goldTableBaseNames {
		tableNames[key] = FormatTableName(baseName, snapshotDate)
	}
	return tableNames
}

func RenderQueryTemplate(query string, variables map[string]interface{}) (string, error) {
	queryTmpl := template.Must(template.New("").Parse(query))

	var dest bytes.Buffer
	if err := queryTmpl.Execute(&dest, variables); err != nil {
		return "", err
	}
	return dest.String(), nil
}

func FormatTableName(tableName string, snapshotDate string) string {
	return fmt.Sprintf("%s_%s", tableName, utils.SnakeCase(snapshotDate))
}

func GenerateAndInsertFromQuery(
	grm *gorm.DB,
	tableName string,
	query string,
	variables []interface{},
	l *zap.Logger,
) error {
	tmpTableName := fmt.Sprintf("%s_tmp", tableName)

	queryWithInsert := fmt.Sprintf("CREATE TABLE %s AS %s", tmpTableName, query)

	_, err := helpers.WrapTxAndCommit(func(tx *gorm.DB) (interface{}, error) {
		queries := []string{
			queryWithInsert,
			fmt.Sprintf(`drop table if exists %s`, tableName),
			fmt.Sprintf(`alter table %s rename to %s`, tmpTableName, tableName),
		}
		for i, query := range queries {
			var res *gorm.DB
			if i == 0 && variables != nil && len(variables) > 0 {
				res = tx.Exec(query, variables...)
			} else {
				res = tx.Exec(query)
			}
			if res.Error != nil {
				l.Sugar().Errorw("Failed to execute query", "query", query, "error", res.Error)
				return nil, res.Error
			}
		}
		return nil, nil
	}, grm, nil)

	return err
}

func DropTableIfExists(grm *gorm.DB, tableName string, l *zap.Logger) error {
	query := fmt.Sprintf(`drop table if exists %s`, tableName)
	res := grm.Exec(query)
	if res.Error != nil {
		l.Sugar().Errorw("Failed to drop table", "table", tableName, "error", res.Error)
		return res.Error
	}
	return nil
}

func FindTableByLikeName(likeName string, grm *gorm.DB, schemaName string) (string, error) {
	if schemaName == "" {
		schemaName = "public"
	}
	query := `
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = @schemaName
				AND table_type='BASE TABLE'
				and table_name like @pattern
			limit 1
		`
	var tname string
	res := grm.Debug().Raw(query,
		sql.Named("schemaName", schemaName),
		sql.Named("pattern", likeName)).
		Scan(&tname)
	if res.Error != nil {
		return "", res.Error
	}
	if tname == "" {
		return "", fmt.Errorf("table not found for key %s", likeName)
	}
	return tname, nil
}

// FindRewardsTableNamesForSearchPatterns finds the table names for the given search patterns
//
// As table names evolve over time due to adding more, the numerical index might change in the constants
// in this file. This makes finding past table names difficult. This function helps to find the table names
// using the base table name and the cutoff date with a wildcard at the front.
func FindRewardsTableNamesForSearchPatterns(patterns map[string]string, cutoffDate string, schemaName string, grm *gorm.DB) (map[string]string, error) {
	results := make(map[string]string)
	for key, pattern := range patterns {
		snakeCaseCutoffDate := utils.SnakeCase(cutoffDate)
		p := fmt.Sprintf("%s_%s", pattern, snakeCaseCutoffDate)
		tname, err := FindTableByLikeName(p, grm, schemaName)
		if err != nil {
			return nil, err
		}
		results[key] = tname
	}
	return results, nil
}
