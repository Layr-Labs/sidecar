package tests

import (
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	sqlite2 "github.com/Layr-Labs/go-sidecar/internal/sqlite"
	"github.com/gocarina/gocsv"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"strings"
)

func GetConfig() *config.Config {
	return config.NewConfig()
}

func GetSqliteDatabaseConnection(l *zap.Logger) (*gorm.DB, error) {
	db, err := sqlite2.NewGormSqliteFromSqlite(sqlite2.NewSqlite(sqlite2.SqliteInMemoryPath, l))
	if err != nil {
		panic(err)
	}
	return db, nil
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

func getMultilineInsertSqlFile(filePath string) ([]string, error) {
	contents, err := getSqlFile(filePath)
	if err != nil {
		return nil, err
	}

	return strings.Split(contents, "\n"), nil
}

func getExpectedResultsJsonFile[T any](filePath string) ([]*T, error) {
	contents, err := os.ReadFile(filePath)

	if err != nil {
		return nil, err
	}

	output := make([]*T, 0)
	if err = json.Unmarshal(contents, &output); err != nil {
		return nil, err
	}
	return output, nil
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
	fmt.Printf("Path: %v\n", path)
	return getSqlFile(path)
}

func GetOperatorAvsRegistrationsSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorAvsRegistrationSnapshots/operatorAvsRegistrations.sql")
	return getSqlFile(path)
}

type ExpectedOperatorAvsRegistrationSnapshot struct {
	Operator string
	Avs      string
	Snapshot string
}

func GetExpectedOperatorAvsSnapshotResults(projectBase string) ([]*ExpectedOperatorAvsRegistrationSnapshot, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorAvsRegistrationSnapshots/operatorAvsSnapshotResults.json")
	return getExpectedResultsJsonFile[ExpectedOperatorAvsRegistrationSnapshot](path)
}

func GetOperatorAvsRestakedStrategiesSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorRestakedStrategies/operatorAvsRestakedStrategies.sql")
	return getSqlFile(path)
}

type ExpectedOperatorAvsSnapshot struct {
	Operator string `csv:"operator"`
	Avs      string `csv:"avs"`
	Strategy string `csv:"strategy"`
	Snapshot string `csv:"snapshot"`
}

func GetExpectedOperatorAvsSnapshots(projectBase string) ([]*ExpectedOperatorAvsSnapshot, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorRestakedStrategies/operatorAvsStrategySnapshotsExpectedResults.csv")
	return getExpectedResultsCsvFile[ExpectedOperatorAvsSnapshot](path)
}

// OperatorShares snapshots
func GetOperatorSharesSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorShareSnapshots/operatorShares.sql")
	return getSqlFile(path)
}

type OperatorShareExpectedResult struct {
	Operator string `csv:"operator"`
	Strategy string `csv:"strategy"`
	Snapshot string `csv:"snapshot"`
	Shares   string `csv:"shares"`
}

func GetOperatorSharesExpectedResults(projectBase string) ([]*OperatorShareExpectedResult, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/operatorShareSnapshots/operatorSharesSnapshotExpectedResults.csv")
	return getExpectedResultsCsvFile[OperatorShareExpectedResult](path)
}

// StakerShareSnapshots
func GetStakerSharesSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/stakerShareSnapshots/stakerShares.sql")
	return getSqlFile(path)
}

type StakerShareExpectedResult struct {
	Staker   string
	Strategy string
	Snapshot string
	Shares   string
}

func GetStakerSharesExpectedResults(projectBase string) ([]*StakerShareExpectedResult, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/stakerShareSnapshots/stakerSharesExpectedResults.json")
	return getExpectedResultsJsonFile[StakerShareExpectedResult](path)
}

// StakerDelegationSnapshots
func GetStakerDelegationsSqlFile(projectBase string) (string, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/stakerDelegationSnapshots/stakerDelegations.sql")
	return getSqlFile(path)
}

type StakerDelegationExpectedResult struct {
	Staker   string
	Operator string
	Snapshot string
}

func GetStakerDelegationExpectedResults(projectBase string) ([]*StakerDelegationExpectedResult, error) {
	path := getTestdataPathFromProjectRoot(projectBase, "/stakerDelegationSnapshots/stakerDelegationExpectedResults.json")
	return getExpectedResultsJsonFile[StakerDelegationExpectedResult](path)
}
