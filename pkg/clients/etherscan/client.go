package etherscan

import (
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"go.uber.org/zap"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var backoffSchedule = []time.Duration{
	1 * time.Second,
	3 * time.Second,
	10 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

func getBaseUrl(network string) string {
	return fmt.Sprintf(`https://%s.etherscan.io/api?`, network)
}

type EtherscanClient struct {
	BaseUrl string
	ApiKeys []string
	client  *http.Client
	logger  *zap.Logger
}

type EtherscanResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Result  json.RawMessage `json:"result"`
}

func NewEtherscanClient(cfg *config.Config, l *zap.Logger) (*EtherscanClient, error) {
	var network string
	switch cfg.Chain {
	case config.Chain_Mainnet:
		network = "api"
	case config.Chain_Holesky:
		network = "api-holesky"
	case config.Chain_Preprod:
		network = "api-holesky"
	default:
		return nil, fmt.Errorf("unknown environment when creating an Etherscan client")
	}

	l.Sugar().Infow("Etherscan client: using network", zap.String("network", network))

	client := &EtherscanClient{
		BaseUrl: getBaseUrl(network),
		ApiKeys: cfg.EtherscanConfig.ApiKeys,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		logger: l,
	}

	return client, nil
}

func wrapError(err error, msg string) (errorWithContext error) {
	if err == nil {
		return nil
	}
	errorWithContext = fmt.Errorf("%s: %w", msg, err)
	return
}

func (ec *EtherscanClient) makeRequest(values url.Values) (*EtherscanResponse, error) {
	values["apikey"] = []string{ec.selectApiKey()}
	
	fullUrl := ec.BaseUrl + values.Encode()

	req, err := http.NewRequest(http.MethodGet, fullUrl, http.NoBody)
	if err != nil {
		return nil, wrapError(err, "failed to create request")
	}

	req.Header.Set("User-Agent", "etherscan-api(Go)")
	req.Header.Set("Content-Type", "application/json; charset=utf-8")

	res, err := ec.client.Do(req)
	if err != nil {
		return nil, wrapError(err, "failed to send request")
	}
	defer res.Body.Close()

	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, wrapError(err, "failed to read response")
	}
	parsedbody := &EtherscanResponse{}
	if err := json.Unmarshal(bodyBytes, &parsedbody); err != nil {
		return nil, wrapError(err, "failed to parse response")
	}

	if res.StatusCode != http.StatusOK {
		return parsedbody, fmt.Errorf("response status %v %s, response body: %s", res.StatusCode, res.Status, parsedbody.Message)
	}

	if parsedbody.Status != "1" {
		return parsedbody, fmt.Errorf("etherscan server: %s", parsedbody.Message)
	}

	return parsedbody, nil
}

func (ec *EtherscanClient) makeRequestWithBackoff(values url.Values) (*EtherscanResponse, error) {
	for _, backoff := range backoffSchedule {

		res, err := ec.makeRequest(values)

		if res == nil {
			ec.logger.Sugar().Errorw("Failed to make request", zap.Error(err))
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

		ec.logger.Info("Rate limit reached, backing off",
			zap.Duration("backoff", backoff),
		)

		time.Sleep(backoff)
	}

	ec.logger.Error("Failed to make request after backoff")
	return nil, fmt.Errorf("failed to make request after backoff")
}

func (ec *EtherscanClient) selectApiKey() string {
	maxNumber := len(ec.ApiKeys)
	randInt := rand.IntN(maxNumber)

	if randInt > maxNumber {
		ec.logger.Sugar().Warnw("Random number is greater than the number of api keys", "randInt", randInt, "maxNumber", maxNumber)
		randInt = 0
	}

	return ec.ApiKeys[randInt]
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
		return "", err
	}

	var decodedOutput string
	err = json.Unmarshal(res.Result, &decodedOutput)
	if err != nil {
		return "", wrapError(err, "failed to decode output")
	}

	return decodedOutput, nil
}
