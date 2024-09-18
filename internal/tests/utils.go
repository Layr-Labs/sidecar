package tests

import (
	"embed"
	"encoding/json"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	sqlite2 "github.com/Layr-Labs/go-sidecar/internal/sqlite"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"strings"
)

func GetConfig() *config.Config {
	return config.NewConfig()
}

const sqliteInMemoryPath = "file::memory:?cache=shared"

func GetSqliteDatabaseConnection(l *zap.Logger) (*gorm.DB, error) {
	db, err := sqlite2.NewGormSqliteFromSqlite(sqlite2.NewSqlite(sqliteInMemoryPath, l))
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

//go:embed testdata
var testData embed.FS

func getSqlFile(name string) (string, error) {
	contents, err := testData.ReadFile(name)

	if err != nil {
		return "", err
	}

	return strings.Trim(string(contents), "\n"), nil
}

func getMultilineInsertSqlFile(name string) ([]string, error) {
	contents, err := getSqlFile(name)
	if err != nil {
		return nil, err
	}

	return strings.Split(contents, "\n"), nil
}

func getExpectedResultsJsonFile[T any](name string) ([]*T, error) {
	contents, err := testData.ReadFile(name)

	if err != nil {
		return nil, err
	}

	output := make([]*T, 0)
	if err = json.Unmarshal(contents, &output); err != nil {
		return nil, err
	}
	return output, nil
}

func GetOperatorAvsRegistrationsSqlFile() ([]string, error) {
	return getMultilineInsertSqlFile("testdata/operatorAvsRegistrationSnapshots/operatorAvsRegistrations.sql")
}

func GetOperatorAvsRegistrationsBlocksSqlFile() ([]string, error) {
	return getMultilineInsertSqlFile("testdata/operatorAvsRegistrationSnapshots/operatorAvsRegistrationsBlocks.sql")
}

type ExpectedOperatorAvsRegistrationSnapshot struct {
	Operator string
	Avs      string
	Snapshot string
}

func GetExpectedOperatorAvsSnapshotResults() ([]*ExpectedOperatorAvsRegistrationSnapshot, error) {
	return getExpectedResultsJsonFile[ExpectedOperatorAvsRegistrationSnapshot]("testdata/operatorAvsRegistrationSnapshots/operatorAvsSnapshotResults.json")
}

func GetOperatorAvsRestakedStrategiesSqlFile() ([]string, error) {
	return getMultilineInsertSqlFile("testdata/operatorRestakedStrategies/operatorAvsRestakedStrategies.sql")
}

type ExpectedOperatorAvsSnapshot struct {
	Operator string
	Avs      string
	Strategy string
	Snapshot string
}

func GetExpectedOperatorAvsSnapshots() ([]*ExpectedOperatorAvsSnapshot, error) {
	return getExpectedResultsJsonFile[ExpectedOperatorAvsSnapshot]("testdata/operatorRestakedStrategies/operatorAvsStrategySnapshotsExpectedResults.json")
}

func GetOperatorSharesSqlFile() ([]string, error) {
	return getMultilineInsertSqlFile("testdata/operatorShareSnapshots/operatorShares.sql")
}

func GetOperatorSharesBlocksSqlFile() (string, error) {
	contents, err := testData.ReadFile("testdata/operatorShareSnapshots/operatorSharesBlocks.sql")

	if err != nil {
		return "", err
	}

	return strings.Trim(string(contents), "\n"), nil
}

type OperatorShareExpectedResult struct {
	Operator string
	Strategy string
	Snapshot string
	Shares   string
}

func GetOperatorSharesExpectedResults() ([]*OperatorShareExpectedResult, error) {
	return getExpectedResultsJsonFile[OperatorShareExpectedResult]("testdata/operatorShareSnapshots/operatorSnapshotExpectedResults.json")
}
