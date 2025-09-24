package aprDataService

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/clients/coingecko"
	"github.com/Layr-Labs/sidecar/pkg/service/baseDataService"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type OperatorStrategyApr struct {
	Strategy string `json:"strategy"`
	Apr      string `json:"apr"`
}

type AprDataService struct {
	baseDataService.BaseDataService
	db              *gorm.DB
	logger          *zap.Logger
	globalConfig    *config.Config
	coingeckoClient *coingecko.Client
}

func NewAprDataService(
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
) *AprDataService {
	var cgClient *coingecko.Client
	if globalConfig.CoingeckoConfig.ApiKey != "" {
		cgClient = coingecko.NewClient(globalConfig.CoingeckoConfig.ApiKey, logger)
	}

	return &AprDataService{
		BaseDataService: baseDataService.BaseDataService{
			DB: db,
		},
		db:              db,
		logger:          logger,
		globalConfig:    globalConfig,
		coingeckoClient: cgClient,
	}
}

var tokenToCoinGeckoID = map[string]string{
	"0xec53bf9167f50cdeb3ae105f56099aaab9061f83": "eigenlayer",
	"0xc43c6bfeda065fe2c4c11765bf838789bd0bb5de": "redstone-oracles",
	"0xc12e4d31e92cedc1ad4c8c23dbce2c5f7cb52998": "skate",
}

// GetDailyOperatorStrategyAprs calculates the daily APR for all strategies for a given operator on a specific date
func (ads *AprDataService) GetDailyOperatorStrategyAprs(ctx context.Context, operatorAddress string, date string) ([]*OperatorStrategyApr, error) {
	operatorAddress = strings.ToLower(operatorAddress)

	// Parse the date
	parsedDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		return nil, fmt.Errorf("invalid date format: %v", err)
	}

	// Format date for CoinGecko API (DD-MM-YYYY format)
	coingeckoDate := parsedDate.Format("02-01-2006")

	// First, get all unique reward tokens for this operator/date to fetch their prices
	var uniqueTokens []string
	tokenQuery := `
		SELECT DISTINCT token 
		FROM staker_operator 
		WHERE operator = @operatorAddress 
		AND DATE(snapshot) = @date
		AND strategy != '0x0000000000000000000000000000000000000000'
	`

	res := ads.db.Raw(tokenQuery,
		sql.Named("operatorAddress", operatorAddress),
		sql.Named("date", parsedDate.Format("2006-01-02")),
	).Scan(&uniqueTokens)

	if res.Error != nil {
		return nil, fmt.Errorf("failed to fetch unique tokens: %v", res.Error)
	}

	// Fetch ETH prices for all tokens
	tokenPrices := make(map[string]float64)
	for _, token := range uniqueTokens {
		coinID, exists := tokenToCoinGeckoID[token]
		if !exists {
			ads.logger.Sugar().Warnw("Token not supported, skipping",
				zap.String("token", token),
			)
			continue
		}

		historicalData, err := ads.coingeckoClient.GetHistoricalDataByCoinID(ctx, coinID, coingeckoDate)
		if err != nil {
			ads.logger.Sugar().Warnw("Failed to fetch price data for token",
				zap.Error(err),
				zap.String("token", token),
				zap.String("coinID", coinID),
			)
			continue
		}

		ethPrice := historicalData.MarketData.CurrentPrice["eth"]
		if ethPrice == 0 {
			ads.logger.Sugar().Warnw("No ETH price found for token",
				zap.String("token", token),
				zap.String("date", coingeckoDate))
			ethPrice = 1 // Fallback
		}

		tokenPrices[token] = ethPrice
		ads.logger.Sugar().Debugw("Retrieved ETH price for token",
			zap.String("token", token),
			zap.String("coinID", coinID),
			zap.Float64("ethPrice", ethPrice),
		)
	}

	query := `
		WITH strategy_token_rewards AS (
			SELECT 
				strategy,
				token,
				-- Sum daily rewards for this operator, strategy, and token
				SUM(amount::numeric) as daily_rewards,
				-- Sum shares that generated these rewards (proxy for staked amount)
				SUM(shares::numeric) as total_shares
			FROM staker_operator
			WHERE 
				operator = @operatorAddress
				AND DATE(snapshot) = @date
				AND strategy != '0x0000000000000000000000000000000000000000'
			GROUP BY strategy, token
		),
		strategy_aggregated AS (
			SELECT 
				strategy,
				-- Sum ETH-converted rewards across all tokens for this strategy
				SUM(
					CASE 
						WHEN token = ANY(@supportedTokens) 
						THEN daily_rewards * (
							CASE token
								` + ads.buildTokenPriceCases(tokenPrices) + `
								ELSE 0
							END
						)
						ELSE 0 
					END
				) as daily_rewards_in_eth,
				-- Use shares from strategy token (assuming 1:1 conversion to underlying asset)
				-- For EIGEN strategy, use EIGEN token shares; convert to ETH using EIGEN/ETH price
				SUM(
					CASE 
						WHEN token = '0xec53bf9167f50cdeb3ae105f56099aaab9061f83'  -- EIGEN token
						THEN total_shares * @eigenEthPrice
						ELSE 0
					END
				) as total_shares_in_eth
			FROM strategy_token_rewards
			GROUP BY strategy
		)
		SELECT 
			strategy,
			-- Calculate APR using ETH-normalized values
			CASE 
				WHEN total_shares_in_eth > 0 
				THEN ROUND((daily_rewards_in_eth / total_shares_in_eth) * 365 * 100, 4)::text
				ELSE '0'
			END as apr
		FROM strategy_aggregated
		WHERE total_shares_in_eth > 0  -- Only include strategies with actual staked amounts
		ORDER BY strategy
	`

	// Prepare supported tokens array and EIGEN ETH price for query
	supportedTokens := make([]string, 0, len(tokenPrices))
	for token := range tokenPrices {
		supportedTokens = append(supportedTokens, token)
	}

	eigenEthPrice := tokenPrices["0xec53bf9167f50cdeb3ae105f56099aaab9061f83"]
	if eigenEthPrice == 0 {
		eigenEthPrice = 1 // Fallback for share conversion
	}

	var results []*OperatorStrategyApr
	res = ads.db.Raw(query,
		sql.Named("operatorAddress", operatorAddress),
		sql.Named("date", parsedDate.Format("2006-01-02")),
		sql.Named("supportedTokens", supportedTokens),
		sql.Named("eigenEthPrice", eigenEthPrice),
	).Scan(&results)

	if res.Error != nil {
		return nil, res.Error
	}

	return results, nil
}

// buildTokenPriceCases generates SQL CASE statements for token price conversions
func (ads *AprDataService) buildTokenPriceCases(tokenPrices map[string]float64) string {
	var cases strings.Builder
	for token, price := range tokenPrices {
		cases.WriteString(fmt.Sprintf("WHEN '%s' THEN %f\n\t\t\t\t\t\t\t\t", token, price))
	}
	return cases.String()
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
