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

	// Get CoinGecko maps (handles unsupported tokens internally)
	strategyToCoinGeckoID, tokenToCoinGeckoID, err := ads.buildCoinGeckoMaps(ctx, strategies, tokens)
	if err != nil {
		return nil, fmt.Errorf("failed to get CoinGecko maps: %w", err)
	}

	// Fetch prices for supported tokens/strategies (maps only contain supported ones)
	coingeckoDate := parsedDate.Format("02-01-2006")

	tokenPrices, err := ads.fetchETHPrices(ctx, tokens, tokenToCoinGeckoID, coingeckoDate, "token")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch token prices: %w", err)
	}

	strategyPrices, err := ads.fetchETHPrices(ctx, strategies, strategyToCoinGeckoID, coingeckoDate, "strategy")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch strategy prices: %w", err)
	}

	// Build and execute query - TypeScript-style: Use rewards as-is (assumes database contains net staker rewards)
	query := `
		WITH strategy_token_rewards AS (
			SELECT 
				strategy,
				token,
				-- Use rewards directly (assumes database contains net staker rewards after commission)
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

// buildCoinGeckoMaps dynamically builds CoinGecko ID mappings, gracefully handling unsupported tokens
func (ads *AprDataService) buildCoinGeckoMaps(ctx context.Context, strategyAddresses []string, additionalTokenAddresses []string) (map[string]string, map[string]string, error) {
	// Special hardcoded token address that should always be treated as a strategy
	const SPECIAL_STRATEGY_TOKEN = "0xc12e4d31e92cedc1ad4c8c23dbce2c5f7cb52998"

	// Hardcoded strategy overrides for cases where underlyingToken doesn't return the desired token
	strategyOverrides := map[string]string{
		"0xacb55c530acdb2849e6d4f36992cd8c9d50ed8f7": "0xec53bf9167f50cdeb3ae105f56099aaab9061f83", // EIGEN strategy override
	}

	// Hardcoded token overrides for special strategy tokens
	tokenOverrides := map[string]string{
		"0xc12e4d31e92cedc1ad4c8c23dbce2c5f7cb52998": "0x61dbbbb552dc893ab3aad09f289f811e67cef285", // Special strategy token override
	}

	// Step 1: Get underlying tokens for strategies
	strategyToToken := make(map[string]string)
	allTokenAddresses := make([]string, 0)

	if len(strategyAddresses) > 0 {
		ads.logger.Sugar().Infow("Getting underlying tokens for strategies",
			"strategies", strategyAddresses,
			"strategyOverrides", strategyOverrides,
			"tokenOverrides", tokenOverrides,
		)

		underlyingTokens, err := ads.strategyCaller.GetUnderlyingTokens(ctx, strategyAddresses)
		if err != nil {
			ads.logger.Sugar().Errorw("Failed to get underlying tokens", "error", err, "strategies", strategyAddresses)
			return nil, nil, fmt.Errorf("failed to get underlying tokens: %w", err)
		}

		ads.logger.Sugar().Infow("Retrieved underlying tokens", "underlyingTokens", underlyingTokens)

		for strategyAddr, tokenAddr := range underlyingTokens {
			normalizedStrategyAddr := strings.ToLower(strategyAddr)

			// Check for hardcoded overrides first
			var finalTokenAddr string
			if overrideToken, hasOverride := strategyOverrides[normalizedStrategyAddr]; hasOverride {
				finalTokenAddr = strings.ToLower(overrideToken)
				ads.logger.Sugar().Infow("Using hardcoded override for strategy",
					"strategy", strategyAddr,
					"contractUnderlyingToken", tokenAddr.Hex(),
					"overrideToken", finalTokenAddr,
				)
			} else if tokenAddr.Hex() != "0x0000000000000000000000000000000000000000" {
				finalTokenAddr = strings.ToLower(tokenAddr.Hex())
				ads.logger.Sugar().Infow("Using contract underlying token for strategy",
					"strategy", strategyAddr,
					"underlyingToken", finalTokenAddr,
				)
			} else {
				ads.logger.Sugar().Warnw("Strategy returned zero address for underlying token", "strategy", strategyAddr)
				continue
			}

			strategyToToken[normalizedStrategyAddr] = finalTokenAddr
			allTokenAddresses = append(allTokenAddresses, finalTokenAddr)

			ads.logger.Sugar().Infow("Added strategy to token mapping",
				"strategy", normalizedStrategyAddr,
				"finalToken", finalTokenAddr,
			)
		}
	}

	// Handle additional token addresses
	resolvedTokens := make([]string, 0)
	strategyTokenAddresses := make([]string, 0) // Tokens that should be treated as strategies
	directTokenAddresses := make([]string, 0)   // Tokens that should be used directly

	// Separate tokens based on whether they should be treated as strategies
	for _, tokenAddr := range additionalTokenAddresses {
		normalizedAddr := strings.ToLower(tokenAddr)
		if normalizedAddr == strings.ToLower(SPECIAL_STRATEGY_TOKEN) {
			// This address should always be treated as a strategy
			strategyTokenAddresses = append(strategyTokenAddresses, tokenAddr)
		} else {
			// This address should be used directly as a token
			directTokenAddresses = append(directTokenAddresses, tokenAddr)
		}
	}

	// Process addresses that should be used directly as tokens
	for _, tokenAddr := range directTokenAddresses {
		resolvedTokens = append(resolvedTokens, strings.ToLower(tokenAddr))
	}

	// Add resolved tokens to all token addresses
	allTokenAddresses = append(allTokenAddresses, resolvedTokens...)

	// For special strategy tokens, we need to track the mapping from original to underlying
	specialTokenMappings := make(map[string]string) // original -> underlying

	// Process addresses that should be treated as strategies (use underlyingToken)
	if len(strategyTokenAddresses) > 0 && ads.strategyCaller != nil {
		underlyingTokens, err := ads.strategyCaller.GetUnderlyingTokens(ctx, strategyTokenAddresses)
		if err != nil {
			ads.logger.Sugar().Warnw("Failed to resolve strategy tokens, using original addresses", "error", err)
			// Fall back to original addresses
			for _, tokenAddr := range strategyTokenAddresses {
				resolvedAddr := strings.ToLower(tokenAddr)
				allTokenAddresses = append(allTokenAddresses, resolvedAddr)
				specialTokenMappings[resolvedAddr] = resolvedAddr // Map to itself as fallback
			}
		} else {
			for _, tokenAddr := range strategyTokenAddresses {
				originalAddr := strings.ToLower(tokenAddr)

				// Check for token overrides first
				var finalTokenAddr string
				if overrideToken, hasOverride := tokenOverrides[originalAddr]; hasOverride {
					finalTokenAddr = strings.ToLower(overrideToken)
					ads.logger.Sugar().Infow("Using hardcoded token override for special strategy token",
						"originalToken", tokenAddr,
						"overrideToken", finalTokenAddr,
					)
				} else if underlyingAddr, exists := underlyingTokens[tokenAddr]; exists && underlyingAddr.Hex() != "0x0000000000000000000000000000000000000000" {
					// Successfully resolved to underlying token
					finalTokenAddr = strings.ToLower(underlyingAddr.Hex())
					ads.logger.Sugar().Infow("Resolved strategy token to underlying token",
						"originalToken", tokenAddr,
						"underlyingToken", finalTokenAddr,
					)
				} else {
					// Failed to resolve, use original address as fallback
					finalTokenAddr = originalAddr
					ads.logger.Sugar().Warnw("Failed to resolve strategy token, using original address",
						"originalToken", tokenAddr,
					)
				}

				// Add the final token to get its CoinGecko ID
				allTokenAddresses = append(allTokenAddresses, finalTokenAddr)

				// Track mapping from original address to final token address
				specialTokenMappings[originalAddr] = finalTokenAddr
			}
		}
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

	// Step 2: Get CoinGecko IDs for tokens (gracefully handle failures)
	tokenToCoinGecko := make(map[string]string)
	underlyingTokenCoinGeckoMap := make(map[string]string) // underlying token -> coingecko ID

	if len(deduplicatedTokens) > 0 {
		ads.logger.Sugar().Infow("Getting CoinGecko IDs for tokens", "tokens", deduplicatedTokens)

		coinGeckoIDs, err := ads.coingeckoClient.GetCoinIDsByContractAddresses(ctx, deduplicatedTokens)
		if err != nil {
			ads.logger.Sugar().Warnw("Failed to get CoinGecko IDs, proceeding with empty token mappings", "error", err)
		} else {
			ads.logger.Sugar().Infow("Retrieved CoinGecko IDs", "coinGeckoIDs", coinGeckoIDs)
		}

		for tokenAddr, coinGeckoID := range coinGeckoIDs {
			if coinGeckoID != "" {
				normalizedAddr := strings.ToLower(tokenAddr)
				underlyingTokenCoinGeckoMap[normalizedAddr] = coinGeckoID
				ads.logger.Sugar().Infow("Mapped token to CoinGecko ID", "token", normalizedAddr, "coinGeckoID", coinGeckoID)
			} else {
				ads.logger.Sugar().Warnw("Empty CoinGecko ID for token", "token", tokenAddr)
			}
		}
	}

	// Build final tokenToCoinGecko mapping
	// For regular tokens, map directly
	for _, tokenAddr := range directTokenAddresses {
		normalizedAddr := strings.ToLower(tokenAddr)
		if coinGeckoID, exists := underlyingTokenCoinGeckoMap[normalizedAddr]; exists {
			tokenToCoinGecko[normalizedAddr] = coinGeckoID
		}
	}

	// For underlying tokens from strategies, also map them to CoinGecko IDs
	for _, underlyingTokenAddr := range strategyToToken {
		if coinGeckoID, exists := underlyingTokenCoinGeckoMap[underlyingTokenAddr]; exists {
			tokenToCoinGecko[underlyingTokenAddr] = coinGeckoID
		}
	}

	// For special strategy tokens, map original address to CoinGecko ID of underlying token
	for originalAddr, underlyingAddr := range specialTokenMappings {
		if coinGeckoID, exists := underlyingTokenCoinGeckoMap[underlyingAddr]; exists {
			tokenToCoinGecko[originalAddr] = coinGeckoID
			ads.logger.Sugar().Infow("Mapped special strategy token to CoinGecko ID",
				"originalToken", originalAddr,
				"underlyingToken", underlyingAddr,
				"coinGeckoID", coinGeckoID,
			)
		}
	}

	// Step 3: Build strategy to CoinGecko ID mapping
	strategyToCoinGecko := make(map[string]string)
	ads.logger.Sugar().Infow("Building strategy to CoinGecko mapping", "strategyToToken", strategyToToken)

	for strategyAddr, tokenAddr := range strategyToToken {
		if coinGeckoID, exists := tokenToCoinGecko[tokenAddr]; exists {
			strategyToCoinGecko[strategyAddr] = coinGeckoID
			ads.logger.Sugar().Infow("Mapped strategy to CoinGecko ID",
				"strategy", strategyAddr,
				"underlyingToken", tokenAddr,
				"coinGeckoID", coinGeckoID,
			)
		} else {
			ads.logger.Sugar().Warnw("No CoinGecko ID found for strategy's underlying token",
				"strategy", strategyAddr,
				"underlyingToken", tokenAddr,
				"availableTokens", func() []string {
					keys := make([]string, 0, len(tokenToCoinGecko))
					for k := range tokenToCoinGecko {
						keys = append(keys, k)
					}
					return keys
				}(),
			)
		}
	}

	// Log the final mappings
	ads.logger.Sugar().Infow("buildCoinGeckoMaps results",
		"strategyToCoinGecko", strategyToCoinGecko,
		"tokenToCoinGecko", tokenToCoinGecko,
		"strategyCount", len(strategyToCoinGecko),
		"tokenCount", len(tokenToCoinGecko),
	)

	return strategyToCoinGecko, tokenToCoinGecko, nil
}

// fetchETHPrices fetches historical ETH prices using CoinGecko
func (ads *AprDataService) fetchETHPrices(
	ctx context.Context,
	addresses []string,
	coinGeckoIDMap map[string]string,
	coingeckoDate string,
	logContext string,
) (map[string]float64, error) {
	prices := make(map[string]float64)

	if ads.coingeckoClient == nil {
		return nil, fmt.Errorf("CoinGecko client not available for %s", logContext)
	}

	for _, address := range addresses {
		coinID, exists := coinGeckoIDMap[address]
		if !exists {
			return nil, fmt.Errorf("%s not supported: %s", logContext, address)
		}

		historicalData, err := ads.coingeckoClient.GetHistoricalDataByCoinID(ctx, coinID, coingeckoDate)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch %s price for %s (coinID: %s): %w", logContext, address, coinID, err)
		}

		ethPrice, exists := historicalData.MarketData.CurrentPrice["eth"]
		if !exists {
			return nil, fmt.Errorf("ETH price not available for %s %s (coinID: %s)", logContext, address, coinID)
		}

		prices[address] = ethPrice
		ads.logger.Sugar().Debugw(fmt.Sprintf("Fetched %s price", logContext),
			"address", address,
			"coinID", coinID,
			"ethPrice", ethPrice,
		)
	}

	return prices, nil
}
