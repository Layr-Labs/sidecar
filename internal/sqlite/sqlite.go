package sqlite

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/types/numbers"
	"go.uber.org/zap"
	"math/big"
	"regexp"

	goSqlite "github.com/mattn/go-sqlite3"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// bytesToHex is a custom SQLite function that converts a JSON byte array to a hex string.
//
// @param jsonByteArray: a JSON byte array, e.g. [1, 2, 3, ...]
// @return: a hex string without a leading 0x, e.g. 78cc56f0700e7ba5055f12...
func bytesToHex(jsonByteArray string) (string, error) {
	jsonBytes := make([]byte, 0)
	err := json.Unmarshal([]byte(jsonByteArray), &jsonBytes)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(jsonBytes), nil
}

type SumBigNumbers struct {
	total *big.Int
}

func NewSumBigNumbers() *SumBigNumbers {
	zero, _ := numbers.NewBig257().SetString("0", 10)
	return &SumBigNumbers{total: zero}
}

func (s *SumBigNumbers) Step(value any) {
	bigValue, success := numbers.NewBig257().SetString(value.(string), 10)
	if !success {
		return
	}
	s.total.Add(s.total, bigValue)
}

func (s *SumBigNumbers) Done() (string, error) {
	return s.total.String(), nil
}

var hasRegisteredExtensions = false

const SqliteInMemoryPath = "file::memory:?cache=shared"

func NewInMemorySqlite(l *zap.Logger) gorm.Dialector {
	return NewSqlite(SqliteInMemoryPath, l)
}

func NewInMemorySqliteWithName(name string, l *zap.Logger) gorm.Dialector {
	path := fmt.Sprintf("file:%s?mode=memory&cache=shared", name)
	return NewSqlite(path, l)
}

func NewSqlite(path string, l *zap.Logger) gorm.Dialector {
	if !hasRegisteredExtensions {
		sql.Register("sqlite3_with_extensions", &goSqlite.SQLiteDriver{
			ConnectHook: func(conn *goSqlite.SQLiteConn) error {
				if err := conn.RegisterAggregator("sum_big", NewSumBigNumbers, true); err != nil {
					l.Sugar().Errorw("Failed to register aggregator sum_big", "error", err)
					return err
				}
				if err := conn.RegisterFunc("bytes_to_hex", bytesToHex, true); err != nil {
					l.Sugar().Errorw("Failed to register function bytes_to_hex", "error", err)
					return err
				}
				return nil
			},
		})
		hasRegisteredExtensions = true
	}

	return &sqlite.Dialector{
		DriverName: "sqlite3_with_extensions",
		DSN:        path,
	}
}

func NewGormSqliteFromSqlite(sqlite gorm.Dialector) (*gorm.DB, error) {
	db, err := gorm.Open(sqlite, &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return nil, err
	}

	// https://phiresky.github.io/blog/2020/sqlite-performance-tuning/
	pragmas := []string{
		`PRAGMA foreign_keys = ON;`,
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA synchronous = normal;`,
		`pragma mmap_size = 30000000000;`,
	}

	for _, pragma := range pragmas {
		res := db.Exec(pragma)
		if res.Error != nil {
			return nil, res.Error
		}
	}

	return db, nil
}

func WrapTxAndCommit[T any](fn func(*gorm.DB) (T, error), db *gorm.DB, tx *gorm.DB) (T, error) {
	exists := tx != nil

	if !exists {
		tx = db.Begin()
	}

	res, err := fn(tx)

	if err != nil && !exists {
		fmt.Printf("Rollback transaction\n")
		tx.Rollback()
	}
	if err == nil && !exists {
		tx.Commit()
	}
	return res, err
}

func IsDuplicateKeyError(err error) bool {
	r := regexp.MustCompile(`UNIQUE constraint failed`)

	return r.MatchString(err.Error())
}
