package rewards

import "database/sql"

const _8_goldFinalQuery = `
insert into gold_table
SELECT
    earner,
    snapshot,
    reward_hash,
    token,
    amount
FROM gold_7_staging
where
    DATE(snapshot) >= @startDate
`

func (rc *RewardsCalculator) GenerateGold8FinalTable(startDate string) error {
	res := rc.grm.Exec(_8_goldFinalQuery, sql.Named("startDate", startDate))
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_final", "error", res.Error)
		return res.Error
	}
	return nil
}

func (rc *RewardsCalculator) Create8GoldTable() error {
	query := `
		create table if not exists gold_table (
			earner TEXT NOT NULL,
			snapshot DATE NOT NULL,
			reward_hash TEXT NOT NULL,
			token TEXT NOT NULL,
			amount TEXT NOT NULL
		)
	`
	res := rc.grm.Exec(query)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to create gold_table", "error", res.Error)
		return res.Error
	}
	return nil
}

// GetNextSnapshotDate retrieves the next snapshot date from the gold_table
// If no snapshot exists, it returns a default date of '1970-01-01'
func (rc *RewardsCalculator) GetNextSnapshotDate() (string, error) {
	var maxDate string
	query := `
		with max_date as (
			select max(snapshot) as snapshot from gold_table
		)
		SELECT
			CASE WHEN md.snapshot IS NULL
			THEN '1970-01-01' ELSE DATE(md.snapshot, '+1 day') END as snapshot
		FROM max_date as md
	`
	res := rc.grm.Raw(query).Scan(&maxDate)
	if res.Error != nil {
		rc.logger.Sugar().Errorw("Failed to get max snapshot date", "error", res.Error)
		return "", res.Error
	}
	return maxDate, nil
}
