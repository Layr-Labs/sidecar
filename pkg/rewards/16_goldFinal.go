package rewards

import (
	"time"

	"github.com/Layr-Labs/sidecar/pkg/rewardsUtils"
	"go.uber.org/zap"
)

const _16_goldFinalQuery = `
INSERT INTO {{.destTableName}} (earner, snapshot, token, amount, reward_hash)
SELECT
    earner,
    snapshot,
    token,
    amount,
    reward_hash
FROM {{.goldStagingTable}}
ON CONFLICT (earner, snapshot, token, reward_hash) DO NOTHING
`

type GoldRow struct {
	Earner     string
	Snapshot   time.Time
	RewardHash string
	Token      string
	Amount     string
}

func (rc *RewardsCalculator) GenerateGold16FinalTable(snapshotDate string, generatedRewardsSnapshotId uint64) error {

	rc.logger.Sugar().Infow("Generating gold final table",
		zap.String("cutoffDate", snapshotDate),
	)

	query, err := rewardsUtils.RenderQueryTemplate(_16_goldFinalQuery, map[string]interface{}{
		"destTableName":              rewardsUtils.RewardsTable_GoldTable,
		"goldStagingTable":           rewardsUtils.RewardsTable_GoldStaging,
		"generatedRewardsSnapshotId": generatedRewardsSnapshotId,
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
