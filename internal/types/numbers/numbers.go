package numbers

import (
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
		return "", err
	}

	return tpd.BigInt().String(), nil
}

// PreNileTokensPerDay calculates the tokens per day for pre-nile rewards, rounded to 15 sigfigs
//
// NOTE: this value is different than postgres and needs more work
func PreNileTokensPerDay(tokensPerDay string) (string, error) {
	tpd, err := decimal.NewFromString(tokensPerDay)
	if err != nil {
		return "", err
	}
	one := (decimal.NewFromInt(10).Pow(decimal.NewFromInt(15))).Sub(decimal.NewFromInt(1))
	two := decimal.NewFromInt(10).Pow(decimal.NewFromInt(15))

	return tpd.Mul(one.Div(two)).BigInt().String(), nil
}
