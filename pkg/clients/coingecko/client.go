package coingecko

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

type Client struct {
	httpClient *http.Client
	apiKey     string
	baseURL    string
	logger     *zap.Logger
}

type HistoricalData struct {
	ID         string               `json:"id"`
	MarketData HistoricalMarketData `json:"market_data"`
}

type HistoricalMarketData struct {
	CurrentPrice map[string]float64 `json:"current_price"`
}

type CoinData struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Symbol string `json:"symbol"`
}

type CoinListItem struct {
	ID        string                 `json:"id"`
	Symbol    string                 `json:"symbol"`
	Name      string                 `json:"name"`
	Platforms map[string]interface{} `json:"platforms,omitempty"`
}

func NewClient(apiKey string, logger *zap.Logger) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		apiKey:  apiKey,
		baseURL: "https://pro-api.coingecko.com/api/v3",
		logger:  logger,
	}
}

// GetHistoricalDataByCoinID fetches historical price data for a specific coin ID and date
// date format should be: "DD-MM-YYYY" (e.g., "30-12-2017")
func (c *Client) GetHistoricalDataByCoinID(ctx context.Context, coinID, date string) (*HistoricalData, error) {
	url := fmt.Sprintf("%s/coins/%s/history", c.baseURL, coinID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add query parameters
	q := req.URL.Query()
	q.Add("date", date)
	q.Add("localization", "false")
	req.URL.RawQuery = q.Encode()

	// Add headers
	req.Header.Set("accept", "application/json")
	req.Header.Set("x-cg-pro-api-key", c.apiKey)

	c.logger.Sugar().Debugw("Making CoinGecko request",
		zap.String("url", req.URL.String()),
		zap.String("coinID", coinID),
		zap.String("date", date),
	)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var historicalData HistoricalData
	if err := json.Unmarshal(body, &historicalData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &historicalData, nil
}

// GetCoinsList fetches a list of all supported coins with ID, name, and symbol
// includePlatform: include platform contract addresses, default: false
// status: filter by coin status (active, inactive, etc.), default: "active"
func (c *Client) GetCoinsList(ctx context.Context, includePlatform bool, status string) ([]CoinListItem, error) {
	url := fmt.Sprintf("%s/coins/list", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add query parameters
	q := req.URL.Query()
	if includePlatform {
		q.Add("include_platform", "true")
	} else {
		q.Add("include_platform", "false")
	}
	if status != "" {
		q.Add("status", status)
	}
	req.URL.RawQuery = q.Encode()

	// Add headers
	req.Header.Set("accept", "application/json")
	req.Header.Set("x-cg-pro-api-key", c.apiKey)

	c.logger.Sugar().Debugw("Making CoinGecko coins list request",
		zap.String("url", req.URL.String()),
		zap.Bool("includePlatform", includePlatform),
		zap.String("status", status),
	)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var coinsList []CoinListItem
	if err := json.Unmarshal(body, &coinsList); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return coinsList, nil
}

// GetCoinIDsByContractAddresses finds CoinGecko IDs for multiple Ethereum contract addresses
// This function searches through all coins and checks specifically for the "ethereum" platform address
func (c *Client) GetCoinIDsByContractAddresses(ctx context.Context, contractAddresses []string) (map[string]string, error) {
	// Get coins list with platform information included
	coinsList, err := c.GetCoinsList(ctx, true, "active")
	if err != nil {
		return nil, fmt.Errorf("failed to get coins list: %w", err)
	}

	// Create a map to store results
	results := make(map[string]string)

	// Create a set of target addresses for O(1) lookup instead of O(n) slices.Contains
	targetAddresses := make(map[string]bool)
	for _, addr := range contractAddresses {
		targetAddresses[addr] = true
	}

	// Search through all coins and check specifically for "ethereum" platform
	for _, coin := range coinsList {
		if coin.Platforms == nil {
			continue
		}

		// Check specifically for the "ethereum" platform
		ethereumAddressInterface, exists := coin.Platforms["ethereum"]
		if !exists || ethereumAddressInterface == nil {
			continue
		}

		// Convert interface{} to string
		addressStr, ok := ethereumAddressInterface.(string)
		if !ok {
			continue
		}

		// O(1) lookup instead of O(n) slices.Contains
		if targetAddresses[addressStr] {
			results[addressStr] = coin.ID

			// Early exit optimization - stop when we've found all addresses
			if len(results) == len(contractAddresses) {
				break
			}
		}
	}

	return results, nil
}
