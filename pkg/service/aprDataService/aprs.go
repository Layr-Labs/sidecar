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
		WITH operator_daily_rewards AS (
			SELECT 
				strategy,
				token,
				SUM(amount::numeric) as daily_reward_amount
			FROM staker_operator
			WHERE 
				operator = @operatorAddress
				AND DATE(snapshot) = @date
				AND reward_type IN ('operator_reward', 'operator_od_reward', 'operator_od_operator_set_reward')
			GROUP BY strategy, token
		),
		-- Get total shares delegated to operator for each strategy on the date
		operator_total_shares AS (
			SELECT 
				oss.strategy,
				oss.shares::numeric as total_shares
			FROM operator_share_snapshots oss
			WHERE 
				oss.operator = @operatorAddress
				AND DATE(oss.snapshot) = @date
		),
		-- Get strategy share prices/values (simplified - using shares as proxy for value)
		-- In a real implementation, you'd need to get the actual token value per share
		strategy_calculations AS (
			SELECT 
				odr.strategy,
				odr.daily_reward_amount,
				COALESCE(ots.total_shares, 0) as total_shares,
				-- Calculate daily APR: (daily_rewards / total_value) * 365 * 100
				-- Using shares as proxy for value - in reality you'd need share price
				CASE 
					WHEN COALESCE(ots.total_shares, 0) > 0 
					THEN (odr.daily_reward_amount / ots.total_shares) * 365 * 100
					ELSE 0
				END as daily_apr
			FROM operator_daily_rewards odr
			LEFT JOIN operator_total_shares ots ON odr.strategy = ots.strategy
		)
		SELECT 
			strategy,
			ROUND(AVG(daily_apr), 4)::text as apr
		FROM strategy_calculations
		WHERE strategy != '0x0000000000000000000000000000000000000000'  -- Exclude null strategy
		GROUP BY strategy
		ORDER BY strategy
	`

	var results []*OperatorStrategyApr
	res := ads.db.Raw(query,
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
		WITH earner_daily_rewards AS (
			SELECT 
				SUM(amount::numeric) as daily_reward_amount
			FROM staker_operator
			WHERE 
				earner = @earnerAddress
				AND strategy = @strategy
				AND DATE(snapshot) = @date
		),
		-- Get earner's shares in the strategy on the date
		earner_shares AS (
			SELECT 
				COALESCE(shares::numeric, 0) as shares
			FROM staker_share_snapshots
			WHERE 
				staker = @earnerAddress
				AND strategy = @strategy
				AND DATE(snapshot) = @date
			LIMIT 1
		),
		-- Calculate APR
		apr_calculation AS (
			SELECT 
				edr.daily_reward_amount,
				es.shares,
				-- Calculate daily APR: (daily_rewards / shares_value) * 365 * 100
				-- Using shares as proxy for value - in reality you'd need share price
				CASE 
					WHEN COALESCE(es.shares, 0) > 0 
					THEN (COALESCE(edr.daily_reward_amount, 0) / es.shares) * 365 * 100
					ELSE 0
				END as daily_apr
			FROM earner_daily_rewards edr
			CROSS JOIN earner_shares es
		)
		SELECT 
			ROUND(daily_apr, 4)::text as apr
		FROM apr_calculation
	`

	var apr string
	res := ads.db.Raw(query,
		sql.Named("earnerAddress", earnerAddress),
		sql.Named("strategy", strategy),
		sql.Named("date", parsedDate.Format("2006-01-02")),
	).Row().Scan(&apr)

	if res != nil {
		ads.logger.Sugar().Errorw("Failed to calculate earner strategy APR",
			"earner", earnerAddress,
			"strategy", strategy,
			"date", date,
			"error", res,
		)
		return "", res
	}

	// Return "0" if no result found
	if apr == "" {
		apr = "0"
	}

	return apr, nil
}
