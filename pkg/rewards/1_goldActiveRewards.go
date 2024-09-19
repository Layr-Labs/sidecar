package rewards

import "database/sql"

var _1_goldActiveRewardsQuery = `

`

func (r *RewardsCalculator) GenerateActiveRewards(snapshotDate string) error {
	res := r.calculationDB.Exec(_1_goldActiveRewardsQuery, sql.Named("snapshotDate", snapshotDate))
	if res.Error != nil {
		return res.Error
	}
	return nil
}
