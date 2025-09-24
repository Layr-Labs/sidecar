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

// GetCoinDataByTokenAddress fetches coin data by contract address
func (c *Client) GetCoinDataByTokenAddress(ctx context.Context, address string) (*CoinData, error) {
	url := fmt.Sprintf("%s/coins/ethereum/contract/%s", c.baseURL, address)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("accept", "application/json")
	req.Header.Set("x-cg-pro-api-key", c.apiKey)

	c.logger.Sugar().Debugw("Making CoinGecko request",
		zap.String("url", req.URL.String()),
		zap.String("address", address),
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
		if resp.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("token not found: %s", address)
		}
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var coinData CoinData
	if err := json.Unmarshal(body, &coinData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &coinData, nil
}
