package numbers

import (
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"math"
	"math/big"
	"testing"
)

func roundToEven(f *big.Float) *big.Float {
	// Get the integer part of the float
	intPart, _ := f.Int(nil)

	// Create a big.Float from the integer part
	rounded := new(big.Float).SetInt(intPart)

	// Calculate the difference (fractional part)
	diff := new(big.Float).Sub(f, rounded)

	// Create a big.Float representing 0.5
	half := big.NewFloat(0.5)

	// Check if the fractional part is exactly 0.5
	if diff.Cmp(half) == 0 {
		// Check if the integer part is even
		intPartMod := new(big.Int).Mod(intPart, big.NewInt(2))
		if intPartMod.Cmp(big.NewInt(0)) != 0 {
			// If odd, round up
			rounded.Add(rounded, big.NewFloat(1))
		}
	} else if diff.Cmp(half) > 0 {
		// If the fractional part is greater than 0.5, round up
		rounded.Add(rounded, big.NewFloat(1))
	}

	return rounded
}

func Test_Numbers(t *testing.T) {
	t.Run("Test that big.Int can produce negative numbers", func(t *testing.T) {
		startingNum := big.Int{}
		startingNum.SetString("10", 10)

		amountToSubtract := big.Int{}
		amountToSubtract.SetString("20", 10)

		assert.Equal(t, "-10", amountToSubtract.Sub(&startingNum, &amountToSubtract).String())
	})
	t.Run("Test that big.NewInt(257) can produce negative numbers", func(t *testing.T) {
		startingNum := big.NewInt(257)
		startingNum.SetString("10", 10)

		amountToSubtract := big.NewInt(257)
		amountToSubtract.SetString("20", 10)

		assert.Equal(t, "-10", amountToSubtract.Sub(startingNum, amountToSubtract).String())
	})
	t.Run("Test that Big257 can produce negative numbers", func(t *testing.T) {
		startingNum, success := NewBig257().SetString("10", 10)
		assert.True(t, success)

		amountToSubtract, success := NewBig257().SetString("20", 10)
		assert.True(t, success)

		assert.Equal(t, "-10", amountToSubtract.Sub(startingNum, amountToSubtract).String())
	})
	t.Run("Test a really big number", func(t *testing.T) {
		startingNum, success := NewBig257().SetString("13389173346000000000000000", 10)
		assert.True(t, success)

		assert.Equal(t, "13389173346000000000000000", startingNum.String())

		amountToSubtract, success := NewBig257().SetString("20", 10)
		assert.True(t, success)

		assert.Equal(t, "13389173345999999999999980", amountToSubtract.Sub(startingNum, amountToSubtract).String())
	})
	t.Run("Test floating point math to see if its correct", func(t *testing.T) {
		t.Skip("math/big is not accurate enough for this test")
		duration := float64(6048000)
		amountStr := "99999999999999999999999999999999999999"
		expectedRawTokensPerDay := "1428571428571428571428571428571428571"
		//expectedFloorValue := "1428571428571428571428571428571428571"
		//expectedRoundedValue := "1428571428571427000000000000000000000"
		// 1428571428571428500000000000000000000

		amount, _ := NewBig257().SetString(amountStr, 10)
		//amount, _, err := big.ParseFloat("99999999999999999999999999999999999999", 10, 38, big.ToPositiveInf)
		//assert.Nil(t, err)

		fmt.Printf("Amount: %+v\n", amount.String())

		amountFloat := big.NewFloat(0)
		amountFloat.SetInt(amount)
		amountFloat.SetMode(big.ToNearestEven)
		//amountFloat.SetPrec(256)

		divisor := big.NewFloat(float64(duration) / 86400)
		divisor.SetMode(big.ToNearestEven)
		fmt.Printf("Divisor: %+v\n", divisor.String())
		tokensPerDay := amountFloat.Quo(amountFloat, divisor)
		tokensPerDay = roundToEven(tokensPerDay)

		rawTokensPerDay := tokensPerDay.Text('f', 0)
		assert.Equal(t, expectedRawTokensPerDay, rawTokensPerDay)

		// We use floor to ensure we are always underesimating total tokens per day
		tokensPerDayFloored := NewBig257()
		tokensPerDayFloored, _ = tokensPerDay.Int(tokensPerDayFloored)

		precision := (math.Pow(10, 15) - float64(1)) / math.Pow(10, 15)

		// Round down to 15 sigfigs for double precision, ensuring know errouneous round up or down
		tokensPerDayDecimal := tokensPerDay.Mul(tokensPerDay, big.NewFloat(precision))

		fmt.Printf("tokensPerDayFloored: %s\n", tokensPerDayFloored.String())
		fmt.Printf("tokensPerDayDecimal: %s\n", tokensPerDayDecimal.String())

	})
	t.Run("Test floating point math to see if its correct with shopspring/decimal", func(t *testing.T) {
		duration := float64(6048000)
		amountStr := "99999999999999999999999999999999999999"
		expectedRawTokensPerDay := "1428571428571428571428571428571428571"
		expectedFloorValue := "1428571428571428571428571428571428571"
		// expectedRoundedValue := "1428571428571427000000000000000000000"
		// 1428571428571428500000000000000000000

		amount, err := decimal.NewFromString(amountStr)
		assert.Nil(t, err)

		fmt.Printf("Amount: %+v\n", amount.String())

		rawTokensPerDay := amount.Div(decimal.NewFromFloat(duration / 86400))
		fmt.Printf("Raw tokens per day: %+v\n", rawTokensPerDay.String())
		// rawTokensPerDayForRounding := amount.Div(decimal.NewFromFloat(duration / 86400))

		assert.Equal(t, expectedRawTokensPerDay, rawTokensPerDay.BigInt().String())

		tokensPerDayFloored := rawTokensPerDay.BigInt()

		assert.Equal(t, expectedFloorValue, tokensPerDayFloored.String())

		one := (decimal.NewFromInt(10).Pow(decimal.NewFromInt(15))).Sub(decimal.NewFromInt(1))
		two := decimal.NewFromInt(10).Pow(decimal.NewFromInt(15))

		fmt.Printf("one/two :%v\n", one.Div(two).String())
		assert.Equal(t, "0.999999999999999", one.Div(two).String())

	})
}
