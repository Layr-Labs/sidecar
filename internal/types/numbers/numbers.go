package numbers

import "C"
import (
	"fmt"
	"github.com/shopspring/decimal"
	"math/big"
)

// NewBig257 returns a new big.Int with a size of 257 bits
// This allows us to fully support math on uint256 numbers
// as well as int256 numbers used for EigenPods.
func NewBig257() *big.Int {
	return big.NewInt(257)
}

// CalcRawTokensPerDay calculates the raw tokens per day for a given amount and duration
// Returns the raw tokens per day in decimal format as a string
func CalcRawTokensPerDay(amountStr string, duration uint64) (string, error) {
	amount, err := decimal.NewFromString(amountStr)
	if err != nil {
		fmt.Printf("CalcRawTokensPerDay Error: %s\n", err)
		return "", err
	}

	rawTokensPerDay := amount.Div(decimal.NewFromFloat(float64(duration) / 86400))

	return rawTokensPerDay.String(), nil
}

// PostNileTokensPerDay calculates the tokens per day for post-nile rewards
// Simply truncates the decimal portion of the of the raw tokens per day
func PostNileTokensPerDay(tokensPerDay string) (string, error) {
	tpd, err := decimal.NewFromString(tokensPerDay)
	if err != nil {
		fmt.Printf("PostNileTokensPerDay Error: %s\n", err)
		return "", err
	}

	return tpd.BigInt().String(), nil
}

func StakerTokenRewards(stakerProportaion string, tokensPerDay string) (string, error) {
	decimal.DivisionPrecision = 38
	sp, err := decimal.NewFromString(stakerProportaion)
	if err != nil {
		return "", err
	}
	tpd, err := decimal.NewFromString(tokensPerDay)
	if err != nil {
		return "", err
	}

	return sp.Mul(tpd).String(), nil
}

func CalculateStakerWeight(multiplier string, shares string) (string, error) {
	m, err := decimal.NewFromString(multiplier)
	if err != nil {
		return "", err
	}
	s, err := decimal.NewFromString(shares)
	if err != nil {
		return "", err
	}

	return m.Mul(s).String(), nil
}
