package coingecko

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestNewClient(t *testing.T) {
	logger := zaptest.NewLogger(t)
	apiKey := "test-api-key"

	client := NewClient(apiKey, logger)

	assert.Equal(t, apiKey, client.apiKey)
	assert.Equal(t, "https://pro-api.coingecko.com/api/v3", client.baseURL)
	assert.NotNil(t, client.httpClient)
	assert.Equal(t, 30*time.Second, client.httpClient.Timeout)
	assert.Equal(t, logger, client.logger)
}

func TestGetHistoricalDataByCoinID_Success(t *testing.T) {
	// Create mock response
	mockResponse := HistoricalData{
		ID: "eigenlayer",
		MarketData: HistoricalMarketData{
			CurrentPrice: map[string]float64{
				"usd": 2.45,
				"eth": 0.0012,
			},
		},
	}

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/coins/eigenlayer/history", r.URL.Path)
		assert.Equal(t, "30-12-2023", r.URL.Query().Get("date"))
		assert.Equal(t, "false", r.URL.Query().Get("localization"))
		assert.Equal(t, "test-api-key", r.Header.Get("x-cg-pro-api-key"))
		assert.Equal(t, "application/json", r.Header.Get("accept"))

		// Send response
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mockResponse)
	}))
	defer server.Close()

	// Create client with test server URL
	logger := zaptest.NewLogger(t)
	client := &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		apiKey:     "test-api-key",
		baseURL:    server.URL,
		logger:     logger,
	}

	// Test the method
	ctx := context.Background()
	result, err := client.GetHistoricalDataByCoinID(ctx, "eigenlayer", "30-12-2023")

	require.NoError(t, err)
	assert.Equal(t, "eigenlayer", result.ID)
	assert.Equal(t, 2.45, result.MarketData.CurrentPrice["usd"])
	assert.Equal(t, 0.0012, result.MarketData.CurrentPrice["eth"])
}

func TestGetHistoricalDataByCoinID_NotFound(t *testing.T) {
	// Create test server that returns 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error": "coin not found"}`))
	}))
	defer server.Close()

	logger := zaptest.NewLogger(t)
	client := &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		apiKey:     "test-api-key",
		baseURL:    server.URL,
		logger:     logger,
	}

	ctx := context.Background()
	result, err := client.GetHistoricalDataByCoinID(ctx, "nonexistent", "30-12-2023")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "API request failed with status 404")
}

func TestGetHistoricalDataByCoinID_InvalidJSON(t *testing.T) {
	// Create test server that returns invalid JSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`invalid json`))
	}))
	defer server.Close()

	logger := zaptest.NewLogger(t)
	client := &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		apiKey:     "test-api-key",
		baseURL:    server.URL,
		logger:     logger,
	}

	ctx := context.Background()
	result, err := client.GetHistoricalDataByCoinID(ctx, "eigenlayer", "30-12-2023")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to unmarshal response")
}

func TestGetCoinDataByTokenAddress_Success(t *testing.T) {
	// Create mock response
	mockResponse := CoinData{
		ID:     "eigenlayer",
		Name:   "EigenLayer",
		Symbol: "EIGEN",
	}

	// Create test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify request
		assert.Equal(t, "GET", r.Method)
		assert.Equal(t, "/coins/ethereum/contract/0xec53bf9167f50cdeb3ae105f56099aaab9061f83", r.URL.Path)
		assert.Equal(t, "test-api-key", r.Header.Get("x-cg-pro-api-key"))
		assert.Equal(t, "application/json", r.Header.Get("accept"))

		// Send response
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mockResponse)
	}))
	defer server.Close()

	// Create client with test server URL
	logger := zaptest.NewLogger(t)
	client := &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		apiKey:     "test-api-key",
		baseURL:    server.URL,
		logger:     logger,
	}

	// Test the method
	ctx := context.Background()
	result, err := client.GetCoinDataByTokenAddress(ctx, "0xec53bf9167f50cdeb3ae105f56099aaab9061f83")

	require.NoError(t, err)
	assert.Equal(t, "eigenlayer", result.ID)
	assert.Equal(t, "EigenLayer", result.Name)
	assert.Equal(t, "EIGEN", result.Symbol)
}

func TestGetCoinDataByTokenAddress_NotFound(t *testing.T) {
	// Create test server that returns 404
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"error": "token not found"}`))
	}))
	defer server.Close()

	logger := zaptest.NewLogger(t)
	client := &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		apiKey:     "test-api-key",
		baseURL:    server.URL,
		logger:     logger,
	}

	ctx := context.Background()
	result, err := client.GetCoinDataByTokenAddress(ctx, "0xinvalidaddress")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "token not found")
}

func TestGetCoinDataByTokenAddress_ServerError(t *testing.T) {
	// Create test server that returns 500
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "internal server error"}`))
	}))
	defer server.Close()

	logger := zaptest.NewLogger(t)
	client := &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		apiKey:     "test-api-key",
		baseURL:    server.URL,
		logger:     logger,
	}

	ctx := context.Background()
	result, err := client.GetCoinDataByTokenAddress(ctx, "0xsome-address")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "API request failed with status 500")
}

func TestGetHistoricalDataByCoinID_ContextCancellation(t *testing.T) {
	// Create test server with delay
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := zaptest.NewLogger(t)
	client := &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		apiKey:     "test-api-key",
		baseURL:    server.URL,
		logger:     logger,
	}

	// Create context that cancels immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := client.GetHistoricalDataByCoinID(ctx, "eigenlayer", "30-12-2023")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "context canceled")
}

func TestFormatDateForAPI(t *testing.T) {
	tests := []struct {
		name     string
		input    time.Time
		expected string
	}{
		{
			name:     "Standard date",
			input:    time.Date(2023, 12, 30, 15, 30, 45, 0, time.UTC),
			expected: "30-12-2023",
		},
		{
			name:     "New Year's Day",
			input:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expected: "01-01-2024",
		},
		{
			name:     "Leap year day",
			input:    time.Date(2024, 2, 29, 12, 0, 0, 0, time.UTC),
			expected: "29-02-2024",
		},
		{
			name:     "Single digit month and day",
			input:    time.Date(2023, 5, 3, 10, 20, 30, 0, time.UTC),
			expected: "03-05-2023",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := FormatDateForAPI(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseAPIDate(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expected    string
		expectError bool
	}{
		{
			name:     "Valid date",
			input:    "2023-12-30",
			expected: "30-12-2023",
		},
		{
			name:     "New Year's Day",
			input:    "2024-01-01",
			expected: "01-01-2024",
		},
		{
			name:     "Leap year day",
			input:    "2024-02-29",
			expected: "29-02-2024",
		},
		{
			name:        "Invalid format - wrong separator",
			input:       "2023/12/30",
			expectError: true,
		},
		{
			name:        "Invalid format - no separators",
			input:       "20231230",
			expectError: true,
		},
		{
			name:        "Invalid date - impossible date",
			input:       "2023-02-30",
			expectError: true,
		},
		{
			name:        "Invalid format - DD-MM-YYYY instead of YYYY-MM-DD",
			input:       "30-12-2023",
			expectError: true,
		},
		{
			name:        "Empty string",
			input:       "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseAPIDate(tt.input)

			if tt.expectError {
				assert.Error(t, err)
				assert.Empty(t, result)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestClient_HTTPClientTimeout(t *testing.T) {
	// Create test server with delay longer than client timeout
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond) // Longer than client timeout
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := zaptest.NewLogger(t)
	client := &Client{
		httpClient: &http.Client{Timeout: 10 * time.Millisecond}, // Very short timeout
		apiKey:     "test-api-key",
		baseURL:    server.URL,
		logger:     logger,
	}

	ctx := context.Background()
	result, err := client.GetHistoricalDataByCoinID(ctx, "eigenlayer", "30-12-2023")

	assert.Error(t, err)
	assert.Nil(t, result)
	// Error should contain timeout information
	assert.True(t, strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "deadline"))
}

func TestClient_EmptyResponse(t *testing.T) {
	// Create test server that returns empty response
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// Empty response body
	}))
	defer server.Close()

	logger := zaptest.NewLogger(t)
	client := &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		apiKey:     "test-api-key",
		baseURL:    server.URL,
		logger:     logger,
	}

	ctx := context.Background()
	result, err := client.GetHistoricalDataByCoinID(ctx, "eigenlayer", "30-12-2023")

	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to unmarshal response")
}

func TestClient_MalformedURL(t *testing.T) {
	logger := zaptest.NewLogger(t)
	client := &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		apiKey:     "test-api-key",
		baseURL:    "://invalid-url", // Malformed URL
		logger:     logger,
	}

	ctx := context.Background()
	result, err := client.GetHistoricalDataByCoinID(ctx, "eigenlayer", "30-12-2023")

	assert.Error(t, err)
	assert.Nil(t, result)
}

// Benchmark tests
func BenchmarkFormatDateForAPI(b *testing.B) {
	testDate := time.Date(2023, 12, 30, 15, 30, 45, 0, time.UTC)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		FormatDateForAPI(testDate)
	}
}

func BenchmarkParseAPIDate(b *testing.B) {
	testDateStr := "2023-12-30"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ParseAPIDate(testDateStr)
	}
}

// Example test to demonstrate usage
func ExampleClient_GetHistoricalDataByCoinID() {
	// This is for documentation purposes only - real usage would require actual API key
	logger := zap.NewNop()
	client := NewClient("your-api-key", logger)

	ctx := context.Background()
	data, err := client.GetHistoricalDataByCoinID(ctx, "eigenlayer", "30-12-2023")
	if err != nil {
		// Handle error
		return
	}

	// Use the data
	_ = data.MarketData.CurrentPrice["usd"]
}

func ExampleFormatDateForAPI() {
	date := time.Date(2023, 12, 30, 0, 0, 0, 0, time.UTC)
	formatted := FormatDateForAPI(date)
	fmt.Println(formatted)
	// Output: 30-12-2023
}
