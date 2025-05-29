package etherscan

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"go.uber.org/zap"
)

var backoffSchedule = []time.Duration{
	1 * time.Second,
	3 * time.Second,
	10 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

type EtherscanClient struct {
	httpClient *http.Client
	Logger     *zap.Logger
	Config     *config.Config
}

type EtherscanResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Result  json.RawMessage `json:"result"`
}

func NewEtherscanClient(hc *http.Client, l *zap.Logger, cfg *config.Config) *EtherscanClient {
	return &EtherscanClient{
		httpClient: hc,
		Logger:     l,
		Config:     cfg,
	}
}

func DefaultHttpClient() *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
	}
}

func (ec *EtherscanClient) getBaseUrl() (string, error) {
	var network string
	switch ec.Config.Chain {
	case config.Chain_Mainnet:
		network = "api"
	case config.Chain_Holesky:
		network = "api-holesky"
	case config.Chain_Sepolia:
		network = "api-sepolia"
	case config.Chain_Preprod:
		network = "api-holesky"
	default:
		return "", fmt.Errorf("unknown environment when making a request using the Etherscan client")
	}
	return fmt.Sprintf("https://%s.etherscan.io/api?", network), nil
}

func (ec *EtherscanClient) makeRequest(values url.Values) (*EtherscanResponse, error) {
	values["apikey"] = []string{ec.Config.EtherscanConfig.ApiKey}

	fullUrl, err := ec.getBaseUrl()
	if err != nil {
		ec.Logger.Sugar().Errorw("Failed to get the Etherscan base URL",
			zap.Error(err),
		)
		return nil, err
	}

	fullUrlWithParams := fullUrl + values.Encode()

	req, err := http.NewRequest(http.MethodGet, fullUrlWithParams, http.NoBody)
	if err != nil {
		ec.Logger.Sugar().Errorw("Failed to create the Etherscan HTTP request",
			zap.Error(err),
		)
		return nil, err
	}

	req.Header.Set("User-Agent", "etherscan-api(Go)")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	res, err := ec.httpClient.Do(req)
	if err != nil {
		ec.Logger.Sugar().Errorw("Failed to perform the Etherscan HTTP request",
			zap.Error(err),
		)
		return nil, err
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		ec.Logger.Sugar().Errorw("Failed to read the Etherscan HTTP response",
			zap.Error(err),
		)
		return nil, err
	}
	parsedbody := &EtherscanResponse{}
	if err := json.Unmarshal(bodyBytes, &parsedbody); err != nil {
		ec.Logger.Sugar().Errorw("Failed to parse json from the Etherscan URL content",
			zap.Error(err),
		)
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return parsedbody, fmt.Errorf("response status %v %s, response body: %s", res.StatusCode, res.Status, parsedbody.Message)
	}

	if parsedbody.Status != "1" {
		return parsedbody, fmt.Errorf("etherscan server: %s", parsedbody.Message)
	}

	ec.Logger.Sugar().Debug("Successfully fetched data from Etherscan")
	return parsedbody, nil
}

func (ec *EtherscanClient) makeRequestWithBackoff(values url.Values) (*EtherscanResponse, error) {
	for _, backoff := range backoffSchedule {

		res, err := ec.makeRequest(values)
		if res == nil {
			ec.Logger.Sugar().Errorw("Failed to make the Etherscan HTTP request",
				zap.Error(err),
			)
			return nil, err
		}

		if res.Status == "1" && err == nil {
			return res, nil
		}

		stringResult := strings.ReplaceAll(string(res.Result), "\"", "")
		r := regexp.MustCompile(`^Max rate limit reached`)

		if !r.MatchString(stringResult) {
			return res, err
		}

		ec.Logger.Info("Rate limit reached, backing off",
			zap.Duration("backoff", backoff),
		)

		time.Sleep(backoff)
	}

	return nil, fmt.Errorf("failed to make the Etherscan request after backoff")
}

func (ec *EtherscanClient) buildBaseUrlParams(module string, action string) url.Values {
	return url.Values{
		"module": []string{module},
		"action": []string{action},
	}
}

func (ec *EtherscanClient) ContractABI(address string) (string, error) {
	baseUrlParams := ec.buildBaseUrlParams("contract", "getabi")
	baseUrlParams.Add("address", address)

	res, err := ec.makeRequestWithBackoff(baseUrlParams)
	if err != nil {
		ec.Logger.Sugar().Errorw("Failed to make the Etherscan HTTP request with backoff",
			zap.Error(err),
		)
		return "", err
	}

	var decodedOutput string
	err = json.Unmarshal(res.Result, &decodedOutput)
	if err != nil {
		ec.Logger.Sugar().Errorw("Failed to decode output from Etherscan URL content",
			zap.Error(err),
		)
		return "", err
	}

	return decodedOutput, nil
}
