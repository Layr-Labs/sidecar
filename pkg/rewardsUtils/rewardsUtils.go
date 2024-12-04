package rewardsUtils

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/Layr-Labs/sidecar/pkg/postgres/helpers"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var (
	Table_1_ActiveRewards           = "gold_1_active_rewards"
	Table_2_StakerRewardAmounts     = "gold_2_staker_reward_amounts"
	Table_3_OperatorRewardAmounts   = "gold_3_operator_reward_amounts"
	Table_4_RewardsForAll           = "gold_4_rewards_for_all"
	Table_5_RfaeStakers             = "gold_5_rfae_stakers"
	Table_6_RfaeOperators           = "gold_6_rfae_operators"
	Table_7_ActiveODRewards         = "gold_7_active_od_rewards"
	Table_8_OperatorODRewardAmounts = "gold_8_operator_od_reward_amounts"
	Table_9_StakerODRewardAmounts   = "gold_9_staker_od_reward_amounts"
	Table_10_AvsODRewardAmounts     = "gold_10_avs_od_reward_amounts"
	Table_11_GoldStaging            = "gold_11_staging"
	Table_12_GoldTable              = "gold_table"

	Sot_6_StakerOperatorStaging = "sot_6_staker_operator_staging"
	Sot_7_StakerOperatorTable   = "staker_operator"
)

var goldTableBaseNames = map[string]string{
	Table_1_ActiveRewards:           Table_1_ActiveRewards,
	Table_2_StakerRewardAmounts:     Table_2_StakerRewardAmounts,
	Table_3_OperatorRewardAmounts:   Table_3_OperatorRewardAmounts,
	Table_4_RewardsForAll:           Table_4_RewardsForAll,
	Table_5_RfaeStakers:             Table_5_RfaeStakers,
	Table_6_RfaeOperators:           Table_6_RfaeOperators,
	Table_7_ActiveODRewards:         Table_7_ActiveODRewards,
	Table_8_OperatorODRewardAmounts: Table_8_OperatorODRewardAmounts,
	Table_9_StakerODRewardAmounts:   Table_9_StakerODRewardAmounts,
	Table_10_AvsODRewardAmounts:     Table_10_AvsODRewardAmounts,
	Table_11_GoldStaging:            Table_11_GoldStaging,
	Table_12_GoldTable:              Table_12_GoldTable,

	Sot_6_StakerOperatorStaging: Sot_6_StakerOperatorStaging,
}

func GetGoldTableNames(snapshotDate string) map[string]string {
	tableNames := make(map[string]string)
	for key, baseName := range goldTableBaseNames {
		tableNames[key] = FormatTableName(baseName, snapshotDate)
	}
	return tableNames
}

func RenderQueryTemplate(query string, variables map[string]string) (string, error) {
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
	variables map[string]interface{},
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
			if i == 0 && variables != nil {
				res = tx.Exec(query, variables)
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
