package tests

import (
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/gocarina/gocsv"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func GenerateTestDbName() (string, error) {
	fileName, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	// remove dashes
	fileNameStr := strings.ReplaceAll(fileName.String(), "-", "")
	return fmt.Sprintf("test_%s_%v", fileNameStr, time.Now().Unix()), nil
}

func GetDbConfigFromEnv() *config.DatabaseConfig {
	return &config.DatabaseConfig{
		Host:     os.Getenv(config.GetEnvVarVar(config.DatabaseHost)),
		Port:     config.StringVarToInt(os.Getenv(config.GetEnvVarVar(config.DatabasePort))),
		User:     os.Getenv(config.GetEnvVarVar(config.DatabaseUser)),
		Password: os.Getenv(config.GetEnvVarVar(config.DatabasePassword)),
		DbName:   os.Getenv(config.GetEnvVarVar(config.DatabaseDbName)),
	}
}

func GetConfig() *config.Config {
	return config.NewConfig()
}

func GetProjectRoot() string {
	return os.Getenv("PROJECT_ROOT")
}

func GetSqliteExtensionsPath() string {
	return fmt.Sprintf("%s/sqlite-extensions/build/lib/libcalculations", GetProjectRoot())
}

func DeleteTestSqliteDB(filePath string) {
	if err := os.Remove(filePath); err != nil {
		panic(err)
	}
}

func ReplaceEnv(newValues map[string]string, previousValues *map[string]string) {
	for k, v := range newValues {
		(*previousValues)[k] = os.Getenv(k)
		os.Setenv(k, v)
	}
}

func RestoreEnv(previousValues map[string]string) {
	for k, v := range previousValues {
		os.Setenv(k, v)
	}
}

func getTestdataPathFromProjectRoot(projectRoot string, fileName string) string {
	p, err := filepath.Abs(fmt.Sprintf("%s/internal/tests/testdata%s", projectRoot, fileName))
	if err != nil {
		panic(err)
	}
	return p
}

func getSqlFile(filePath string) (string, error) {
	contents, err := os.ReadFile(filePath)

	if err != nil {
		return "", err
	}

	return strings.Trim(string(contents), "\n"), nil
}
func getExpectedResultsCsvFile[T any](filePath string) ([]*T, error) {
	results := make([]*T, 0)
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if err := gocsv.UnmarshalFile(file, &results); err != nil {
		panic(err)
	}
	return results, nil
}

func GetAllBlocksSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/allBlocks.sql")
	return getSqlFile(path)
}

func GetOperatorAvsRegistrationsSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorAvsRegistrationSnapshots/operatorAvsRegistrations.sql")
	return getSqlFile(path)
}

type ExpectedOperatorAvsRegistrationSnapshot struct {
	Operator string `csv:"operator"`
	Avs      string `csv:"avs"`
	Snapshot string `csv:"snapshot"`
}

func GetExpectedOperatorAvsSnapshotResults(projectBase string) ([]*ExpectedOperatorAvsRegistrationSnapshot, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorAvsRegistrationSnapshots/expectedResults.csv")
	return getExpectedResultsCsvFile[ExpectedOperatorAvsRegistrationSnapshot](path)
}

func GetOperatorAvsRestakedStrategiesSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorRestakedStrategies/operatorRestakedStrategies.sql")
	return getSqlFile(path)
}

type ExpectedOperatorAvsSnapshot struct {
	Operator string `csv:"operator"`
	Avs      string `csv:"avs"`
	Strategy string `csv:"strategy"`
	Snapshot string `csv:"snapshot"`
}

func GetExpectedOperatorAvsSnapshots(projectBase string) ([]*ExpectedOperatorAvsSnapshot, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorRestakedStrategies/expectedResults.csv")
	return getExpectedResultsCsvFile[ExpectedOperatorAvsSnapshot](path)
}

func GetOperatorShareDeltasSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorShares/operatorShareDeltas.sql")
	return getSqlFile(path)
}

// OperatorShares snapshots
func GetOperatorSharesSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorShareSnapshots/operatorShares.sql")
	return getSqlFile(path)
}

type OperatorShareExpectedResult struct {
	Operator        string `csv:"operator"`
	Strategy        string `csv:"strategy"`
	TransactionHash string `csv:"transaction_hash"`
	LogIndex        uint64 `csv:"snapshot"`
	Shares          string `csv:"shares"`
	BlockNumber     uint64 `csv:"block_number"`
}

func GetOperatorShareExpectedResults(projectBase string) ([]*OperatorShareExpectedResult, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorShares/expectedResults.csv")
	return getExpectedResultsCsvFile[OperatorShareExpectedResult](path)
}

type OperatorShareSnapshotsExpectedResult struct {
	Operator string `csv:"operator"`
	Strategy string `csv:"strategy"`
	Snapshot string `csv:"snapshot"`
	Shares   string `csv:"shares"`
}

func GetOperatorShareSnapshotsExpectedResults(projectBase string) ([]*OperatorShareSnapshotsExpectedResult, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorShareSnapshots/expectedResults.csv")
	return getExpectedResultsCsvFile[OperatorShareSnapshotsExpectedResult](path)
}

// StakerShareSnapshots
func GetStakerSharesSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/stakerShareSnapshots/stakerShares.sql")
	return getSqlFile(path)
}

func GetStakerShareDeltasSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/stakerShares/stakerShareDeltas.sql")
	return getSqlFile(path)
}

type StakerShareSnapshotExpectedResult struct {
	Staker   string `csv:"staker"`
	Strategy string `csv:"strategy"`
	Snapshot string `csv:"snapshot"`
	Shares   string `csv:"shares"`
}

func GetStakerSharesSnapshotsExpectedResults(projectBase string) ([]*StakerShareSnapshotExpectedResult, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/stakerShareSnapshots/expectedResults.csv")
	return getExpectedResultsCsvFile[StakerShareSnapshotExpectedResult](path)
}

type StakerShareExpectedResult struct {
	Staker          string `csv:"staker"`
	Strategy        string `csv:"strategy"`
	TransactionHash string `csv:"transaction_hash"`
	LogIndex        uint64 `csv:"snapshot"`
	Shares          string `csv:"shares"`
	BlockNumber     uint64 `csv:"block_number"`
}

func GetStakerSharesExpectedResults(projectBase string) ([]*StakerShareExpectedResult, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/stakerShares/expectedResults.csv")
	return getExpectedResultsCsvFile[StakerShareExpectedResult](path)
}

// StakerDelegationSnapshots
func GetStakerDelegationsSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/stakerDelegationSnapshots/stakerDelegations.sql")
	return getSqlFile(path)
}

type StakerDelegationExpectedResult struct {
	Staker   string `csv:"staker"`
	Operator string `csv:"operator"`
	Snapshot string `csv:"snapshot"`
}

func GetStakerDelegationExpectedResults(projectBase string) ([]*StakerDelegationExpectedResult, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/stakerDelegationSnapshots/expectedResults.csv")
	return getExpectedResultsCsvFile[StakerDelegationExpectedResult](path)
}

// CombinedRewards
func GetCombinedRewardsSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/combinedRewards/combinedRewards.sql")
	return getSqlFile(path)
}

type GoldStagingExpectedResult struct {
	Earner     string `csv:"earner"`
	Snapshot   string `csv:"snapshot"`
	RewardHash string `csv:"reward_hash"`
	Token      string `csv:"token"`
	Amount     string `csv:"amount"`
}

func GetGoldExpectedResults(projectBase string, snapshotDate string) ([]*GoldStagingExpectedResult, error) {
	path := getTestdataPathFromProjectRoot(projectBase, fmt.Sprintf("/7_goldStaging/expectedResults_%s.csv", snapshotDate))
	return getExpectedResultsCsvFile[GoldStagingExpectedResult](path)
}

func GetStakerSharesTransactionLogsSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/stakerShares/stakerSharesTransactionLogs.sql")
	return getSqlFile(path)
}

func GetOperatorSharesTransactionLogsSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorShares/operatorSharesTransactionLogs.sql")
	return getSqlFile(path)
}

func GetAvsOperatorsTransactionLogsSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/avsOperators/avsOperatorRegistrationTransactionLogs.sql")
	return getSqlFile(path)
}

func LargeTestsEnabled() bool {
	return os.Getenv("TEST_REWARDS") == "true" || os.Getenv("TEST_LARGE") == "true"
}
