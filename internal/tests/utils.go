package tests

import (
	"embed"
	"encoding/json"
	"github.com/Layr-Labs/go-sidecar/internal/config"
	sqlite2 "github.com/Layr-Labs/go-sidecar/internal/sqlite"
	"gorm.io/gorm"
	"os"
	"strings"
)

func GetConfig() *config.Config {
	return config.NewConfig()
}

const sqliteInMemoryPath = "file::memory:?cache=shared"

func GetSqliteDatabaseConnection() (*gorm.DB, error) {
	db, err := sqlite2.NewGormSqliteFromSqlite(sqlite2.NewSqlite(sqliteInMemoryPath))
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

func GetOperatorAvsRegistrationsSqlFile() ([]string, error) {
	contents, err := testData.ReadFile("testdata/operatorAvsRegistrations.sql")

	if err != nil {
		return nil, err
	}

	return strings.Split(strings.Trim(string(contents), "\n"), "\n"), nil
}

func GetOperatorAvsRegistrationsBlocksSqlFile() ([]string, error) {
	contents, err := testData.ReadFile("testdata/operatorAvsRegistrationsBlocks.sql")

	if err != nil {
		return nil, err
	}

	return strings.Split(strings.Trim(string(contents), "\n"), "\n"), nil
}

func GetOperatorAvsRestakedStrategiesSqlFile() ([]string, error) {
	contents, err := testData.ReadFile("testdata/operatorAvsRestakedStrategies.sql")

	if err != nil {
		return nil, err
	}

	return strings.Split(strings.Trim(string(contents), "\n"), "\n"), nil
}

type ExpectedOperatorAvsRegistrationSnapshot struct {
	Operator string
	Avs      string
	Snapshot string
}

func GetExpectedOperatorAvsSnapshotResults() ([]*ExpectedOperatorAvsRegistrationSnapshot, error) {
	contents, err := testData.ReadFile("testdata/operatorAvsSnapshotResults.json")

	if err != nil {
		return nil, err
	}

	output := make([]*ExpectedOperatorAvsRegistrationSnapshot, 0)
	if err = json.Unmarshal(contents, &output); err != nil {
		return nil, err
	}
	return output, nil
}

type ExpectedOperatorAvsSnapshot struct {
	Operator string
	Avs      string
	Strategy string
	Snapshot string
}

func GetExpectedOperatorAvsSnapshots() ([]*ExpectedOperatorAvsSnapshot, error) {
	contents, err := testData.ReadFile("testdata/operatorAvsStrategySnapshotsExpectedResults.json")

	if err != nil {
		return nil, err
	}

	output := make([]*ExpectedOperatorAvsSnapshot, 0)
	if err = json.Unmarshal(contents, &output); err != nil {
		return nil, err
	}
	return output, nil
}
