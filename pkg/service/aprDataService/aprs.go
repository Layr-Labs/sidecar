package aprDataService

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/clients/coingecko"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller"
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
	strategyCaller  contractCaller.IStrategyCaller
}

func NewAprDataService(
	db *gorm.DB,
	logger *zap.Logger,
	globalConfig *config.Config,
	strategyCaller contractCaller.IStrategyCaller,
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
		strategyCaller:  strategyCaller,
	}
}

// GetDailyOperatorStrategyAprs calculates the daily APR for all strategies for a given operator
func (ads *AprDataService) GetDailyOperatorStrategyAprs(ctx context.Context, operatorAddress string, date string) ([]*OperatorStrategyApr, error) {
	operatorAddress = strings.ToLower(operatorAddress)

	// Parse the date
	parsedDate, err := time.Parse("2006-01-02", date)
	if err != nil {
		return nil, fmt.Errorf("invalid date format: %v", err)
	}

	// Check if data exists
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
	).Count(&count)

	if res.Error != nil {
		return nil, res.Error
	}

	if count == 0 {
		ads.logger.Sugar().Warnw("No data found for operator and date",
			zap.String("operator", operatorAddress),
			zap.String("date", date),
		)
		return []*OperatorStrategyApr{}, nil
	}

	// Get strategies and tokens for this date
	strategiesQuery := `
		SELECT DISTINCT strategy 
		FROM staker_operator 
		WHERE operator = @operatorAddress 
		AND DATE(snapshot) = @date
		AND strategy != '0x0000000000000000000000000000000000000000'
	`

	var strategies []string
	res = ads.db.Raw(strategiesQuery,
		sql.Named("operatorAddress", operatorAddress),
		sql.Named("date", parsedDate.Format("2006-01-02")),
	).Scan(&strategies)

	if res.Error != nil {
		return nil, res.Error
	}

	// Get unique tokens
	tokensQuery := `
		SELECT DISTINCT token 
		FROM staker_operator 
		WHERE operator = @operatorAddress 
		AND DATE(snapshot) = @date
		AND token != '0x0000000000000000000000000000000000000000'
	`

	var tokens []string
	res = ads.db.Raw(tokensQuery,
		sql.Named("operatorAddress", operatorAddress),
		sql.Named("date", parsedDate.Format("2006-01-02")),
	).Scan(&tokens)

	if res.Error != nil {
		return nil, res.Error
	}

	// Get shares data for strategies
	strategyShares := make(map[string]*big.Int)
	sharesQuery := `
		SELECT strategy, MAX(shares) as shares
		FROM staker_operator 
		WHERE operator = @operatorAddress 
		AND DATE(snapshot) = @date
		AND strategy != '0x0000000000000000000000000000000000000000'
		GROUP BY strategy
	`

	type StrategyShares struct {
		Strategy string `gorm:"column:strategy"`
		Shares   string `gorm:"column:shares"`
	}

	var sharesResults []StrategyShares
	res = ads.db.Raw(sharesQuery,
		sql.Named("operatorAddress", operatorAddress),
		sql.Named("date", parsedDate.Format("2006-01-02")),
	).Scan(&sharesResults)

	if res.Error != nil {
		return nil, res.Error
	}

	for _, result := range sharesResults {
		shares := new(big.Int)
		shares.SetString(result.Shares, 10)
		strategyShares[result.Strategy] = shares
	}

	// Get underlying amounts
	var strategyAmounts map[string]*big.Int
	if ads.strategyCaller != nil {
		strategyAmounts, err = ads.strategyCaller.GetSharesToUnderlyingAmounts(ctx, strategyShares)
		if err != nil {
			ads.logger.Sugar().Warnw("Failed to get underlying amounts, using shares", "error", err)
			strategyAmounts = strategyShares
		}
	} else {
		strategyAmounts = strategyShares
	}

	// Get CoinGecko maps
	strategyToCoinGeckoID, tokenToCoinGeckoID, err := ads.buildCoinGeckoMaps(ctx, strategies, tokens)
	if err != nil {
		return nil, fmt.Errorf("failed to get CoinGecko maps: %w", err)
	}

	// Fetch prices
	coingeckoDate := parsedDate.Format("02-01-2006")

	tokenPrices, err := ads.fetchETHPrices(ctx, tokens, tokenToCoinGeckoID, coingeckoDate, "token", true)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch token prices: %w", err)
	}

	strategyPrices, err := ads.fetchETHPrices(ctx, strategies, strategyToCoinGeckoID, coingeckoDate, "strategy", true)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch strategy prices: %w", err)
	}

	// Build and execute query
	query := `
		WITH strategy_token_rewards AS (
			SELECT 
				strategy,
				token,
				SUM(amount::numeric) as daily_rewards,
				MAX(shares::numeric) as total_shares
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
				(
					CASE strategy
						` + ads.buildStrategyAmountCases(strategyAmounts) + `
						ELSE MAX(total_shares)
					END
				) / POWER(10, 18) * (
					CASE strategy
						` + ads.buildStrategyPriceCases(strategyPrices) + `
						ELSE 1.0
					END
				) as total_shares_in_eth
			FROM strategy_token_rewards
			GROUP BY strategy
		)
		SELECT 
			strategy,
			CASE 
				WHEN total_shares_in_eth > 0 
				THEN ROUND(((daily_rewards_in_eth / total_shares_in_eth) * 365 * 100)::numeric, 4)::text
				ELSE '0'
			END as apr
		FROM strategy_aggregated
		WHERE total_shares_in_eth > 0
		ORDER BY strategy
	`

	// Prepare supported tokens array
	supportedRewardTokens := make([]string, 0, len(tokenPrices))
	for token := range tokenPrices {
		supportedRewardTokens = append(supportedRewardTokens, token)
	}
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

// Helper functions for building SQL cases
func (ads *AprDataService) buildTokenPriceCases(tokenPrices map[string]float64) string {
	var cases strings.Builder
	for token, price := range tokenPrices {
		cases.WriteString(fmt.Sprintf("WHEN '%s' THEN %f\n\t\t\t\t\t\t\t\t", token, price))
	}
	if cases.Len() == 0 {
		cases.WriteString("WHEN '0x0000000000000000000000000000000000000000' THEN 0\n\t\t\t\t\t\t\t\t")
	}
	return cases.String()
}

func (ads *AprDataService) buildStrategyPriceCases(strategyPrices map[string]float64) string {
	var cases strings.Builder
	for strategy, price := range strategyPrices {
		cases.WriteString(fmt.Sprintf("WHEN '%s' THEN %f\n\t\t\t\t\t\t\t", strategy, price))
	}
	if cases.Len() == 0 {
		cases.WriteString("WHEN '0x0000000000000000000000000000000000000000' THEN 1.0\n\t\t\t\t\t\t\t")
	}
	return cases.String()
}

func (ads *AprDataService) buildStrategyAmountCases(strategyAmounts map[string]*big.Int) string {
	var cases strings.Builder
	for strategy, amount := range strategyAmounts {
		cases.WriteString(fmt.Sprintf("WHEN '%s' THEN %s\n\t\t\t\t\t\t\t", strategy, amount.String()))
	}
	if cases.Len() == 0 {
		cases.WriteString("WHEN '0x0000000000000000000000000000000000000000' THEN 0\n\t\t\t\t\t\t\t")
	}
	return cases.String()
}

// buildCoinGeckoMaps dynamically builds CoinGecko ID mappings
func (ads *AprDataService) buildCoinGeckoMaps(ctx context.Context, strategyAddresses []string, additionalTokenAddresses []string) (map[string]string, map[string]string, error) {
	// Step 1: Get underlying tokens for strategies
	strategyToToken := make(map[string]string)
	allTokenAddresses := make([]string, 0)

	if len(strategyAddresses) > 0 {
		underlyingTokens, err := ads.strategyCaller.GetUnderlyingTokens(ctx, strategyAddresses)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get underlying tokens: %w", err)
		}

		for strategyAddr, tokenAddr := range underlyingTokens {
			if tokenAddr.Hex() != "0x0000000000000000000000000000000000000000" {
				tokenAddrStr := strings.ToLower(tokenAddr.Hex())
				strategyToToken[strings.ToLower(strategyAddr)] = tokenAddrStr
				allTokenAddresses = append(allTokenAddresses, tokenAddrStr)
			}
		}
	}

	// Add additional tokens
	for _, tokenAddr := range additionalTokenAddresses {
		allTokenAddresses = append(allTokenAddresses, strings.ToLower(tokenAddr))
	}

	// Deduplicate tokens
	uniqueTokens := make(map[string]bool)
	for _, token := range allTokenAddresses {
		uniqueTokens[token] = true
	}
	deduplicatedTokens := make([]string, 0, len(uniqueTokens))
	for token := range uniqueTokens {
		deduplicatedTokens = append(deduplicatedTokens, token)
	}

	// Step 2: Get CoinGecko IDs for tokens
	tokenToCoinGecko := make(map[string]string)
	if len(deduplicatedTokens) > 0 {
		coinGeckoIDs, err := ads.coingeckoClient.GetCoinIDsByContractAddresses(ctx, deduplicatedTokens)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get CoinGecko IDs: %w", err)
		}

		for tokenAddr, coinGeckoID := range coinGeckoIDs {
			if coinGeckoID != "" {
				tokenToCoinGecko[strings.ToLower(tokenAddr)] = coinGeckoID
			}
		}
	}

	// Step 3: Build strategy to CoinGecko ID mapping
	strategyToCoinGecko := make(map[string]string)
	for strategyAddr, tokenAddr := range strategyToToken {
		if coinGeckoID, exists := tokenToCoinGecko[tokenAddr]; exists {
			strategyToCoinGecko[strategyAddr] = coinGeckoID
		}
	}

	return strategyToCoinGecko, tokenToCoinGecko, nil
}

// fetchETHPrices fetches historical ETH prices using CoinGecko
func (ads *AprDataService) fetchETHPrices(
	ctx context.Context,
	addresses []string,
	coinGeckoIDMap map[string]string,
	coingeckoDate string,
	logContext string,
	useFallback bool,
) (map[string]float64, error) {
	prices := make(map[string]float64)

	if ads.coingeckoClient == nil {
		if useFallback {
			ads.logger.Sugar().Warnw("CoinGecko client not available, using fallbacks",
				"logContext", logContext,
			)
			for _, address := range addresses {
				prices[address] = 1.0
			}
			return prices, nil
		}
		return nil, fmt.Errorf("CoinGecko client not available and fallbacks not enabled for %s", logContext)
	}

	for _, address := range addresses {
		coinID, exists := coinGeckoIDMap[address]
		if !exists {
			if useFallback {
				ads.logger.Sugar().Warnw(fmt.Sprintf("%s not supported, using fallback", logContext),
					"address", address,
				)
				prices[address] = 1.0
			}
			continue
		}

		historicalData, err := ads.coingeckoClient.GetHistoricalDataByCoinID(ctx, coinID, coingeckoDate)
		if err != nil {
			if useFallback {
				ads.logger.Sugar().Warnw(fmt.Sprintf("Failed to fetch %s price, using fallback", logContext),
					"address", address,
					"coinID", coinID,
					"error", err,
				)
				prices[address] = 1.0
			}
			continue
		}

		if ethPrice, exists := historicalData.MarketData.CurrentPrice["eth"]; exists {
			prices[address] = ethPrice
			ads.logger.Sugar().Debugw(fmt.Sprintf("Fetched %s price", logContext),
				"address", address,
				"coinID", coinID,
				"ethPrice", ethPrice,
			)
		} else if useFallback {
			ads.logger.Sugar().Warnw(fmt.Sprintf("ETH price not available for %s, using fallback", logContext),
				"address", address,
				"coinID", coinID,
			)
			prices[address] = 1.0
		}
	}

	return prices, nil
}
