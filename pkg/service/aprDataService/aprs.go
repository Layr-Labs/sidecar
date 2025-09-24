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
	"github.com/lib/pq"
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

// Map of strategy addresses to their CoinGecko IDs (from mainnet_ethereum_strategy_category.csv)
var strategyToCoinGeckoID = map[string]string{
	"0x93c4b944d05dfe6df7645a86cd2206016c51564d": "staked-ether",                // stETH
	"0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2": "rocket-pool-eth",             // rETH
	"0x54945180db7943c0ed0fee7edab2bd24620256bc": "coinbase-wrapped-staked-eth", // cbETH
	"0x9d7ed45ee2e8fc5482fa2428f15c971e6369011d": "stader-ethx",                 // ETHx
	"0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff": "ankreth",                     // ankrETH
	"0xa4c637e0f704745d182e4d38cab7e7485321d059": "origin-ether",                // oETH
	"0x57ba429517c3473b6d34ca9acd56c0e735b94c02": "stakewise-v3-oseth",          // osETH
	"0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6": "sweth",                       // swETH
	"0x7ca911e83dabf90c90dd3de5411a10f1a6112184": "wrapped-beacon-eth",          // wBETH
	"0x8ca7a5d6f3acd3a7a8bc468a8cd0fb14b6bd28b6": "staked-frax-ether",           // sfrxETH
	"0xae60d8180437b5c34bb956822ac2710972584473": "liquid-staked-ethereum",      // lsETH
	"0x298afb19a105d59e74658c4c334ff360bade6dd2": "mantle-staked-ether",         // mETH
	"0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0": "ethereum",                    // ETH (Beacon)
	"0xacb55c530acdb2849e6d4f36992cd8c9d50ed8f7": "eigenlayer",                  // EIGEN
}

// Map of reward token addresses to their CoinGecko IDs
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

	// Check if data exists for the given operator and date
	var count int64
	dataCheckQuery := `
		SELECT COUNT(*) 
		FROM staker_operator 
		WHERE operator = @operatorAddress 
		AND DATE(snapshot) = @date
		AND strategy != '0x0000000000000000000000000000000000000000'
	`

	res := ads.db.Raw(dataCheckQuery,
		sql.Named("operatorAddress", operatorAddress),
		sql.Named("date", parsedDate.Format("2006-01-02")),
	).Scan(&count)
	if res.Error != nil {
		return nil, fmt.Errorf("failed to check data availability: %v", res.Error)
	}

	if count == 0 {
		return nil, fmt.Errorf("date %s is not available", date)
	}

	// Format date for CoinGecko API (DD-MM-YYYY format)
	coingeckoDate := parsedDate.Format("02-01-2006")

	// Get all unique reward tokens and strategies for this operator/date
	type TokenStrategy struct {
		Token    string `json:"token"`
		Strategy string `json:"strategy"`
	}

	var uniqueTokensStrategies []TokenStrategy
	tokenStrategyQuery := `
		SELECT DISTINCT token, strategy
		FROM staker_operator 
		WHERE operator = @operatorAddress 
		AND DATE(snapshot) = @date
		AND strategy != '0x0000000000000000000000000000000000000000'
	`

	res = ads.db.Raw(tokenStrategyQuery,
		sql.Named("operatorAddress", operatorAddress),
		sql.Named("date", parsedDate.Format("2006-01-02")),
	).Scan(&uniqueTokensStrategies)
	if res.Error != nil {
		return nil, fmt.Errorf("failed to fetch unique tokens and strategies: %v", res.Error)
	}

	// Extract unique tokens and strategies
	uniqueTokensSet := make(map[string]bool)
	uniqueStrategiesSet := make(map[string]bool)
	for _, ts := range uniqueTokensStrategies {
		uniqueTokensSet[ts.Token] = true
		uniqueStrategiesSet[ts.Strategy] = true
	}

	var uniqueTokens []string
	for token := range uniqueTokensSet {
		uniqueTokens = append(uniqueTokens, token)
	}

	var uniqueStrategies []string
	for strategy := range uniqueStrategiesSet {
		uniqueStrategies = append(uniqueStrategies, strategy)
	}

	// Fetch ETH prices for all reward tokens
	tokenPrices, err := ads.fetchETHPrices(ctx, uniqueTokens, tokenToCoinGeckoID, coingeckoDate, "reward token", false)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch reward token prices: %v", err)
	}

	// Fetch ETH prices for all strategies
	strategyPrices, err := ads.fetchETHPrices(ctx, uniqueStrategies, strategyToCoinGeckoID, coingeckoDate, "strategy", true)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch strategy prices: %v", err)
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
				-- Convert rewards from wei to ETH using decimals (18), then multiply by token ETH price
				SUM(
					CASE 
						WHEN token = ANY(@supportedRewardTokens) 
						THEN (daily_rewards / POWER(10, 18)) * (
							CASE token
								` + ads.buildTokenPriceCases(tokenPrices) + `
								ELSE 0
							END
						)
						ELSE 0 
					END
				) as daily_rewards_in_eth,
				-- Convert shares to underlying tokens (1:1 ratio), apply decimals (18), then convert to ETH  
				-- Formula: shares * 1.0 / 10^18 * strategyEthPrice
				MAX(total_shares) / POWER(10, 18) * (
					CASE strategy
						` + ads.buildStrategyPriceCases(strategyPrices) + `
						ELSE 1.0 -- Fallback for unsupported strategies
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
				THEN ROUND(((daily_rewards_in_eth / total_shares_in_eth) * 365 * 100)::numeric, 4)::text
				ELSE '0'
			END as apr
		FROM strategy_aggregated
		WHERE total_shares_in_eth > 0  -- Only include strategies with actual staked amounts
		ORDER BY strategy
	`

	// Prepare supported tokens array for query
	supportedRewardTokens := make([]string, 0, len(tokenPrices))
	for token := range tokenPrices {
		supportedRewardTokens = append(supportedRewardTokens, token)
	}
	// Ensure array is never empty to prevent SQL issues with ANY() function
	if len(supportedRewardTokens) == 0 {
		supportedRewardTokens = append(supportedRewardTokens, "0x0000000000000000000000000000000000000000")
	}

	var results []*OperatorStrategyApr
	res = ads.db.Raw(query,
		sql.Named("operatorAddress", operatorAddress),
		sql.Named("date", parsedDate.Format("2006-01-02")),
		sql.Named("supportedRewardTokens", pq.Array(supportedRewardTokens)),
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
	// Ensure there's always at least one WHEN clause to prevent empty CASE statements
	if cases.Len() == 0 {
		cases.WriteString("WHEN '0x0000000000000000000000000000000000000000' THEN 0\n\t\t\t\t\t\t\t\t")
	}
	return cases.String()
}

// buildStrategyPriceCases generates SQL CASE statements for strategy price conversions
func (ads *AprDataService) buildStrategyPriceCases(strategyPrices map[string]float64) string {
	var cases strings.Builder
	for strategy, price := range strategyPrices {
		cases.WriteString(fmt.Sprintf("WHEN '%s' THEN %f\n\t\t\t\t\t\t\t", strategy, price))
	}
	// Ensure there's always at least one WHEN clause to prevent empty CASE statements
	if cases.Len() == 0 {
		cases.WriteString("WHEN '0x0000000000000000000000000000000000000000' THEN 1.0\n\t\t\t\t\t\t\t")
	}
	return cases.String()
}

// fetchETHPrices fetches historical ETH prices for a list of addresses using their CoinGecko IDs
func (ads *AprDataService) fetchETHPrices(
	ctx context.Context,
	addresses []string,
	coinGeckoIDMap map[string]string,
	coingeckoDate string,
	logContext string,
	useFallback bool,
) (map[string]float64, error) {
	prices := make(map[string]float64)

	// Check if CoinGecko client is available
	if ads.coingeckoClient == nil {
		if useFallback {
			ads.logger.Sugar().Warnw("CoinGecko client not available (missing API key), using fallbacks for all prices",
				zap.String("logContext", logContext),
			)
			// Use fallback prices for all addresses
			for _, address := range addresses {
				prices[address] = 1.0
			}
			return prices, nil
		} else {
			return nil, fmt.Errorf("CoinGecko client not available (missing API key) and fallbacks not enabled for %s", logContext)
		}
	}

	for _, address := range addresses {
		coinID, exists := coinGeckoIDMap[address]
		if !exists {
			if useFallback {
				ads.logger.Sugar().Warnw(fmt.Sprintf("%s not supported, using 1.0 ETH fallback", logContext),
					zap.String("address", address),
				)
				prices[address] = 1.0
			} else {
				ads.logger.Sugar().Warnw(fmt.Sprintf("%s not supported, skipping", logContext),
					zap.String("address", address),
				)
			}
			continue
		}

		historicalData, err := ads.coingeckoClient.GetHistoricalDataByCoinID(ctx, coinID, coingeckoDate)
		if err != nil {
			if useFallback {
				ads.logger.Sugar().Warnw(fmt.Sprintf("Failed to fetch price data for %s, using 1.0 ETH fallback", logContext),
					zap.Error(err),
					zap.String("address", address),
					zap.String("coinID", coinID),
				)
				prices[address] = 1.0
			} else {
				ads.logger.Sugar().Warnw(fmt.Sprintf("Failed to fetch price data for %s", logContext),
					zap.Error(err),
					zap.String("address", address),
					zap.String("coinID", coinID),
				)
			}
			continue
		}

		ethPrice := historicalData.MarketData.CurrentPrice["eth"]
		if ethPrice == 0 {
			fallbackPrice := 1.0
			ads.logger.Sugar().Warnw(fmt.Sprintf("No ETH price found for %s, using %v ETH fallback", logContext, fallbackPrice),
				zap.String("address", address),
				zap.String("date", coingeckoDate))
			ethPrice = fallbackPrice
		}

		prices[address] = ethPrice
		ads.logger.Sugar().Debugw(fmt.Sprintf("Retrieved ETH price for %s", logContext),
			zap.String("address", address),
			zap.String("coinID", coinID),
			zap.Float64("ethPrice", ethPrice),
		)
	}

	return prices, nil
}
