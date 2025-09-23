package aprDataService

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/service/baseDataService"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type AprDataService struct {
	baseDataService.BaseDataService
	db           *gorm.DB
	logger       *zap.Logger
	globalConfig *config.Config
}

func NewAprDataService(
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) *AprDataService {
	return &AprDataService{
		BaseDataService: baseDataService.BaseDataService{
			DB: db,
		},
		db:           db,
		logger:       logger,
		globalConfig: globalConfig,
	}
}

type OperatorStrategyApr struct {
	Strategy string
	Apr      string
}

// GetDailyOperatorStrategyAprs calculates the daily APR for all strategies for a given operator on a specific date
func (ads *AprDataService) GetDailyOperatorStrategyAprs(ctx context.Context, operatorAddress string, date string) ([]*OperatorStrategyApr, error) {
	operatorAddress = strings.ToLower(operatorAddress)

	// Parse the date
	parsedDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		return nil, fmt.Errorf("invalid date format: %v", err)
	}

	query := `
		WITH strategy_rewards_and_shares AS (
			SELECT 
				strategy,
				-- Sum daily rewards for this operator and strategy
				SUM(amount::numeric) as daily_rewards,
				-- Sum shares that generated these rewards (proxy for staked amount)
				SUM(shares::numeric) as total_shares
			FROM staker_operator
			WHERE 
				strategy = @eigenStrategyAddress  -- EIGEN strategy
    			AND token = @eigenTokenAddress  -- EIGEN token
				AND operator = @operatorAddress
				AND DATE(snapshot) = @date
				AND strategy != '0x0000000000000000000000000000000000000000'
			GROUP BY strategy
		)
		SELECT 
			strategy,
			-- Calculate APR: (daily_rewards / staked_amount) * 365 * 100
			CASE 
				WHEN total_shares > 0 
				THEN ROUND((daily_rewards / total_shares) * 365 * 100, 4)::text
				ELSE '0'
			END as apr
		FROM strategy_rewards_and_shares
		ORDER BY strategy
	`

	var results []*OperatorStrategyApr
	res := ads.db.Raw(query,
		sql.Named("eigenStrategyAddress", "0xacb55c530acdb2849e6d4f36992cd8c9d50ed8f7"),
		sql.Named("eigenTokenAddress", "0xec53bf9167f50cdeb3ae105f56099aaab9061f83"),
		sql.Named("operatorAddress", operatorAddress),
		sql.Named("date", parsedDate.Format("2006-01-02")),
	).Scan(&results)
	if res.Error != nil {
		return nil, res.Error
	}

	return results, nil
}

// GetDailyAprForEarnerStrategy calculates the daily APR for a specific earner and strategy on a specific date
func (ads *AprDataService) GetDailyAprForEarnerStrategy(ctx context.Context, earnerAddress string, strategy string, date string) (string, error) {
	earnerAddress = strings.ToLower(earnerAddress)
	strategy = strings.ToLower(strategy)

	// Parse the date
	parsedDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		return "", fmt.Errorf("invalid date format: %v", err)
	}

	query := `
		SELECT 
			-- Calculate APR: (daily_rewards / staked_amount) * 365 * 100
			CASE 
				WHEN SUM(shares::numeric) > 0 
				THEN ROUND((SUM(amount::numeric) / SUM(shares::numeric)) * 365 * 100, 4)::text
				ELSE '0'
			END as apr
		FROM staker_operator
		WHERE 
			earner = @earnerAddress
			AND strategy = @strategy
			AND DATE(snapshot) = @date
	`

	var apr string
	res := ads.db.Raw(query,
		sql.Named("earnerAddress", earnerAddress),
		sql.Named("strategy", strategy),
		sql.Named("date", parsedDate.Format("2006-01-02")),
	).Row().Scan(&apr)
	if res != nil {
		return "", res
	}

	// Return "0" if no result found
	if apr == "" {
		apr = "0"
	}

	return apr, nil
}
