package aprDataService

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/clients/coingecko"
	"github.com/Layr-Labs/sidecar/pkg/contractCaller"
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

// AprQueryParams holds parameters for APR queries
type AprQueryParams struct {
	OperatorAddress string
	EarnerAddress   string
	Strategy        string
	Date            string
}

type AprQueryResult struct {
	Strategy    string `gorm:"column:strategy"`
	RewardTypes string `gorm:"column:reward_types"`
	Apr         string `gorm:"column:apr"`
}

// GetDailyOperatorStrategyAprs calculates the daily APR for all strategies for a given operator
func (ads *AprDataService) GetDailyOperatorStrategyAprs(ctx context.Context, operatorAddress string, date string) ([]*OperatorStrategyApr, error) {
	params := AprQueryParams{
		OperatorAddress: operatorAddress,
		Date:            date,
	}

	queryResults, err := ads.calculateDailyAprs(ctx, params)
	if err != nil {
		return nil, err
	}

	var aprResults []*OperatorStrategyApr
	for _, queryResult := range queryResults {
		aprResults = append(aprResults, &OperatorStrategyApr{
			Strategy: queryResult.Strategy,
			Apr:      queryResult.Apr,
		})
	}

	return aprResults, nil
}

// GetDailyAprForEarnerStrategy calculates the daily APR for a specific earner and strategy combination
func (ads *AprDataService) GetDailyAprForEarnerStrategy(ctx context.Context, earnerAddress string, strategy string, date string) (string, error) {
	params := AprQueryParams{
		EarnerAddress: earnerAddress,
		Strategy:      strategy,
		Date:          date,
	}

	queryResults, err := ads.calculateDailyAprs(ctx, params)
	if err != nil {
		return "", err
	}

	if len(queryResults) == 0 {
		return "0", nil
	}

	// Return the APR for the single strategy (there should only be one result)
	return queryResults[0].Apr, nil
}

// calculateDailyAprs is a shared internal function for calculating APRs
func (ads *AprDataService) calculateDailyAprs(ctx context.Context, params AprQueryParams) ([]AprQueryResult, error) {
	parsedDate, err := time.Parse("2006-01-02", params.Date)
	if err != nil {
		return nil, fmt.Errorf("invalid date format: %v", err)
	}

	// Build WHERE clause conditions based on provided parameters
	var whereConditions []string
	namedParams := make(map[string]interface{})

	if params.OperatorAddress != "" {
		whereConditions = append(whereConditions, "operator = @operatorAddress")
		namedParams["operatorAddress"] = strings.ToLower(params.OperatorAddress)
	}

	if params.EarnerAddress != "" {
		whereConditions = append(whereConditions, "earner = @earnerAddress")
		namedParams["earnerAddress"] = strings.ToLower(params.EarnerAddress)
	}

	if params.Strategy != "" {
		whereConditions = append(whereConditions, "strategy = @strategy")
		namedParams["strategy"] = strings.ToLower(params.Strategy)
	}

	whereConditions = append(whereConditions, "DATE(snapshot) = @date")
	whereConditions = append(whereConditions, "strategy != '0x0000000000000000000000000000000000000000'")
	namedParams["date"] = parsedDate.Format("2006-01-02")

	whereClause := strings.Join(whereConditions, " AND ")

	// Check if data exists
	var exists bool
	dataCheckQuery := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 
			FROM staker_operator 
			WHERE %s
		)
	`, whereClause)

	res := ads.db.Raw(dataCheckQuery, namedParams).Scan(&exists)
	if res.Error != nil {
		return nil, res.Error
	}

	if !exists {
		logFields := []zap.Field{zap.String("date", params.Date)}
		if params.OperatorAddress != "" {
			logFields = append(logFields, zap.String("operator", params.OperatorAddress))
		}
		if params.EarnerAddress != "" {
			logFields = append(logFields, zap.String("earner", params.EarnerAddress))
		}
		if params.Strategy != "" {
			logFields = append(logFields, zap.String("strategy", params.Strategy))
		}
		ads.logger.Warn("No data found for the given parameters", logFields...)
		return []AprQueryResult{}, nil
	}

	// Query for strategy and token data
	query := fmt.Sprintf(`
		SELECT 
			strategy,
			token,
			MAX(shares) as shares
		FROM staker_operator 
		WHERE %s
		GROUP BY strategy, token
	`, whereClause)

	var results []struct {
		Strategy string `gorm:"column:strategy"`
		Token    string `gorm:"column:token"`
		Shares   string `gorm:"column:shares"`
	}
	res = ads.db.Raw(query, namedParams).Scan(&results)
	if res.Error != nil {
		return nil, res.Error
	}

	// Extract strategies, tokens, and strategyShares from combined results
	strategyShares := make(map[string]*big.Int)
	strategiesSet := make(map[string]bool)
	tokensSet := make(map[string]bool)

	for _, result := range results {
		shares := new(big.Int)
		shares.SetString(result.Shares, 10)
		strategyShares[result.Strategy] = shares
		strategiesSet[result.Strategy] = true
		if result.Token != "0x0000000000000000000000000000000000000000" {
			tokensSet[result.Token] = true
		}
	}

	// Convert sets to slices
	var strategies []string
	for strategy := range strategiesSet {
		strategies = append(strategies, strategy)
	}

	var tokens []string
	for token := range tokensSet {
		tokens = append(tokens, token)
	}

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

	strategyToCoinGeckoID, tokenToCoinGeckoID, err := ads.buildCoinGeckoMaps(ctx, strategies, tokens)
	if err != nil {
		return nil, fmt.Errorf("failed to get CoinGecko maps: %w", err)
	}

	coingeckoDate := parsedDate.Format("02-01-2006")

	tokenPrices, err := ads.fetchETHPrices(ctx, tokens, tokenToCoinGeckoID, coingeckoDate, "token")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch token prices: %w", err)
	}

	strategyPrices, err := ads.fetchETHPrices(ctx, strategies, strategyToCoinGeckoID, coingeckoDate, "strategy")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch strategy prices: %w", err)
	}

	tokenDecimals := ads.getTokenDecimals(ctx, tokens)

	aprQuery := fmt.Sprintf(`
		WITH strategy_token_rewards AS (
			SELECT 
				strategy,
				token,
				reward_type,
				-- Use rewards directly from single date, only staker rewards
				SUM(amount::numeric) as daily_rewards,
				MAX(shares::numeric) as total_shares
			FROM staker_operator
			WHERE 
				%s
				-- Only include staker rewards, not operator rewards
				AND reward_type IN ('staker_reward', 'staker_od_reward', 'staker_od_operator_set_reward', 'rfae_staker')
			GROUP BY strategy, token, reward_type
		),
		strategy_aggregated AS (
			SELECT 
				strategy,
				STRING_AGG(DISTINCT reward_type, ', ') as reward_types,
				SUM(
					(daily_rewards / (
						CASE token
							%s
							ELSE POWER(10, 18)
						END
					)) * (
						CASE token
							%s
							ELSE 0
						END
					)
				) as daily_rewards_in_eth,
				(
					CASE strategy
						%s
						ELSE MAX(total_shares)
					END
				) / POWER(10, 18) * (
					CASE strategy
						%s
						ELSE 1.0
					END
				) as total_shares_in_eth
			FROM strategy_token_rewards
			GROUP BY strategy
		)
		SELECT 
			strategy,
			reward_types,
			CASE 
				WHEN total_shares_in_eth > 0 
				THEN ROUND(((daily_rewards_in_eth / total_shares_in_eth) * 365 * 100)::numeric, 4)::text
				ELSE '0'
			END as apr
		FROM strategy_aggregated
		WHERE total_shares_in_eth > 0
		ORDER BY strategy
	`, whereClause,
		ads.buildTokenDecimalsCases(tokenDecimals),
		ads.buildTokenPriceCases(tokenPrices),
		ads.buildStrategyAmountCases(strategyAmounts),
		ads.buildStrategyPriceCases(strategyPrices))

	var queryResults []AprQueryResult
	res = ads.db.Raw(aprQuery, namedParams).Scan(&queryResults)
	if res.Error != nil {
		return nil, res.Error
	}

	return queryResults, nil
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

// getTokenDecimals fetches token decimals from database
func (ads *AprDataService) getTokenDecimals(ctx context.Context, tokens []string) map[string]int {
	if len(tokens) == 0 {
		return make(map[string]int)
	}

	// Only include tokens with non-18 decimals (18 is the default)
	knownDecimals := map[string]int{
		"0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6, // USDC
		"0xdac17f958d2ee523a2206206994597c13d831ec7": 6, // USDT (if it appears)
	}

	decimals := make(map[string]int)

	for _, token := range tokens {
		token = strings.ToLower(token)
		if knownDecimal, exists := knownDecimals[token]; exists {
			decimals[token] = knownDecimal
		} else {
			// Default to 18 decimals for all other tokens
			decimals[token] = 18
		}
	}

	return decimals
}

func (ads *AprDataService) buildTokenDecimalsCases(tokenDecimals map[string]int) string {
	var cases strings.Builder
	for token, decimals := range tokenDecimals {
		cases.WriteString(fmt.Sprintf("WHEN '%s' THEN POWER(10, %d)\n\t\t\t\t\t\t\t", token, decimals))
	}
	if cases.Len() == 0 {
		cases.WriteString("WHEN '0x0000000000000000000000000000000000000000' THEN POWER(10, 18)\n\t\t\t\t\t\t\t")
	}
	return cases.String()
}

// buildCoinGeckoMaps dynamically builds CoinGecko ID mappings, gracefully handling unsupported tokens
func (ads *AprDataService) buildCoinGeckoMaps(ctx context.Context, strategyAddresses []string, tokenAddresses []string) (map[string]string, map[string]string, error) {
	// Step 1: Get underlying tokens for strategies and collect all unique tokens
	strategyToToken := make(map[string]string)
	tokens := make(map[string]bool)

	ads.logger.Sugar().Infow("Getting underlying tokens for strategies", "strategies", strategyAddresses)

	underlyingTokens, err := ads.strategyCaller.GetUnderlyingTokens(ctx, strategyAddresses)
	if err != nil {
		ads.logger.Sugar().Errorw("Failed to get underlying tokens", "error", err, "strategies", strategyAddresses)
		return nil, nil, fmt.Errorf("failed to get underlying tokens: %w", err)
	}

	// Process strategy underlying tokens
	for strategyAddr, tokenAddr := range underlyingTokens {
		strategyAddrLower := strings.ToLower(strategyAddr)
		tokenAddrLower := strings.ToLower(tokenAddr.Hex())
		strategyToToken[strategyAddrLower] = tokenAddrLower
		tokens[tokenAddrLower] = true
	}

	for _, tokenAddr := range tokenAddresses {
		tokens[strings.ToLower(tokenAddr)] = true
	}

	uniqueTokens := make([]string, 0, len(tokens))
	for token := range tokens {
		uniqueTokens = append(uniqueTokens, token)
	}

	// Step 2: Get CoinGecko IDs for tokens (gracefully handle failures)
	tokenToCoinGecko := make(map[string]string)
	underlyingTokenCoinGeckoMap := make(map[string]string) // underlying token -> coingecko ID

	if len(uniqueTokens) > 0 {
		coinGeckoIDs, err := ads.coingeckoClient.GetCoinIDsByContractAddresses(ctx, uniqueTokens)
		if err != nil {
			ads.logger.Sugar().Warnw("Failed to get CoinGecko IDs, proceeding with empty token mappings", "error", err)
		}

		for tokenAddr, coinGeckoID := range coinGeckoIDs {
			if coinGeckoID != "" {
				normalizedAddr := strings.ToLower(tokenAddr)
				underlyingTokenCoinGeckoMap[normalizedAddr] = coinGeckoID
			}
		}
	}

	for _, tokenAddr := range uniqueTokens {
		if coinGeckoID, exists := underlyingTokenCoinGeckoMap[tokenAddr]; exists {
			tokenToCoinGecko[tokenAddr] = coinGeckoID
		}
	}

	// Step 3: Build strategy to CoinGecko ID mapping
	strategyToCoinGecko := make(map[string]string)

	for strategyAddr, tokenAddr := range strategyToToken {
		if coinGeckoID, exists := tokenToCoinGecko[tokenAddr]; exists {
			strategyToCoinGecko[strategyAddr] = coinGeckoID
		}
	}

	// Add direct CoinGecko ID mappings
	strategyToCoinGecko["0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0"] = "ethereum"   // Beacon Chain ETH strategy -> ethereum
	strategyToCoinGecko["0xacb55c530acdb2849e6d4f36992cd8c9d50ed8f7"] = "eigenlayer" // EIGEN strategy -> [fill in CoinGecko ID]

	tokenToCoinGecko["0xc12e4d31e92cedc1ad4c8c23dbce2c5f7cb52998"] = "skate" // Special strategy token -> [fill in CoinGecko ID]

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
