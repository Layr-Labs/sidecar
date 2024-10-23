package sqlite

import (
	"fmt"
	sqlite2 "github.com/Layr-Labs/go-sidecar/internal/sqlite"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"time"
)

func GetInMemorySqliteDatabaseConnection(l *zap.Logger) (*gorm.DB, error) {
	db, err := sqlite2.NewGormSqliteFromSqlite(sqlite2.NewSqlite(&sqlite2.SqliteConfig{
		Path:           sqlite2.SqliteInMemoryPath,
		ExtensionsPath: []string{tests.GetSqliteExtensionsPath()},
	}, l))
	if err != nil {
		panic(err)
	}
	return db, nil
}

func GetFileBasedSqliteDatabaseConnection(l *zap.Logger) (string, *gorm.DB, error) {
	fileName, err := uuid.NewUUID()
	if err != nil {
		panic(err)
	}
	basePath := fmt.Sprintf("%ssidecardb-%s-%d", os.TempDir(), fileName, time.Now().Unix())
	if err := os.MkdirAll(basePath, os.ModePerm); err != nil {
		return "", nil, err
	}

	filePath := fmt.Sprintf("%s/test.db", basePath)
	fmt.Printf("File path: %s\n", filePath)
	db, err := sqlite2.NewGormSqliteFromSqlite(sqlite2.NewSqlite(&sqlite2.SqliteConfig{
		Path:           filePath,
		ExtensionsPath: []string{tests.GetSqliteExtensionsPath()},
	}, l))
	if err != nil {
		panic(err)
	}
	return filePath, db, nil
}
