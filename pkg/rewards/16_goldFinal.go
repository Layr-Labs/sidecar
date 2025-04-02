package rewards

import (
	"time"

	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _16_goldFinalQuery = `
insert into gold_table
SELECT
    earner,
    snapshot,
    reward_hash,
    token,
    amount
FROM {{.goldStagingTable}}
`

type GoldRow struct {
	Earner     string
	Snapshot   time.Time
	RewardHash string
	Token      string
	Amount     string
}

func (rc *RewardsCalculator) GenerateGold16FinalTable(snapshotDate string) error {
	allTableNames := rewardsUtils.GetGoldTableNames(snapshotDate)

	rc.logger.Sugar().Infow("Generating gold final table",
		zap.String("cutoffDate", snapshotDate),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_16_goldFinalQuery, map[string]interface{}{
		"goldStagingTable": allTableNames[rewardsUtils.Table_15_GoldStaging],
	})
	if err != nil {
		rc.logger.Sugar().Errorw("Failed to render query template", "error", err)
		return err
	}

	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_final", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) ListGoldRows() ([]*GoldRow, error) {
	var goldRows []*GoldRow
	res := rc.grm.Raw("select * from gold_table").Scan(&goldRows)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to list gold rows", "error", res.Error)
		return nil, res.Error
	}
	return goldRows, nil
}
