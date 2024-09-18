package sqlite

import (
	"encoding/hex"
	"fmt"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/stretchr/testify/assert"
	"math/big"
	"strings"
	"testing"
)

func Test_Sqlite(t *testing.T) {
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: true})

	t.Run("Should use the bytesToHex function", func(t *testing.T) {
		query := `
			with json_values as (
				select 
					cast('{"newWithdrawalRoot": [218, 200, 138, 86, 38, 9, 156, 119, 73, 13, 168, 40, 209, 43, 238, 83, 234, 177, 230, 73, 120, 205, 255, 143, 255, 216, 51, 209, 137, 100, 163, 233] }' as text) as json_col
				from (select 1)
			)
			select
				bytes_to_hex(json_extract(json_col, '$.newWithdrawalRoot')) AS withdrawal_hex
			from json_values
			limit 1
		`
		s := NewSqlite("file::memory:?cache=shared", l)
		grm, err := NewGormSqliteFromSqlite(s)
		assert.Nil(t, err)

		type results struct {
			WithdrawalHex string
		}

		hexValue := &results{}
		res := grm.Raw(query).Scan(&hexValue)

		expectedBytes := []byte{218, 200, 138, 86, 38, 9, 156, 119, 73, 13, 168, 40, 209, 43, 238, 83, 234, 177, 230, 73, 120, 205, 255, 143, 255, 216, 51, 209, 137, 100, 163, 233}

		assert.Nil(t, res.Error)
		assert.Equal(t, strings.ToLower(hex.EncodeToString(expectedBytes)), hexValue.WithdrawalHex)
	})
	t.Run("Should sum two really big numbers that are stored as strings", func(t *testing.T) {
		shares1 := "1670000000000000000000"
		shares2 := "1670000000000000000000"

		type shares struct {
			Shares string
		}

		operatorShares := []*shares{
			&shares{
				Shares: shares1,
			},
			&shares{
				Shares: shares2,
			},
		}

		s := NewSqlite("file::memory:?cache=shared", l)
		grm, err := NewGormSqliteFromSqlite(s)
		assert.Nil(t, err)

		createQuery := `
			create table shares (
				shares TEXT NOT NULL
			)
		`
		res := grm.Exec(createQuery)
		assert.Nil(t, res.Error)

		res = grm.Model(&shares{}).Create(&operatorShares)
		assert.Nil(t, res.Error)

		query := `
			select
				sum_big(shares) as total
			from shares
		`
		var total string
		res = grm.Raw(query).Scan(&total)
		assert.Nil(t, res.Error)
		fmt.Printf("Total: %s\n", total)

		shares1Big, _ := new(big.Int).SetString(shares1, 10)
		shares2Big, _ := new(big.Int).SetString(shares2, 10)

		expectedTotal := shares1Big.Add(shares1Big, shares2Big)

		assert.Equal(t, expectedTotal.String(), total)
	})
}
