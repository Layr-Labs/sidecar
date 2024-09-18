package tests

import (
	"os"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	sqlite2 "github.com/Layr-Labs/go-sidecar/internal/sqlite"
	"gorm.io/gorm"
)

func GetConfigFromOptions(options *config.Options) *config.Config {
	cfg := config.NewConfig(options)
	return cfg
}

func GetConfig() *config.Config {
	testOpts := &config.Options{
		Network:     "holesky",
		Environment: "testnet",
	}

	cfg := config.NewConfig(testOpts)
	return cfg
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
