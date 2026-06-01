package base

import (
	"encoding/json"
	"testing"
)

func TestParseLogOutputAsType(t *testing.T) {
	type sample struct {
		Staker string      `json:"staker"`
		Shares json.Number `json:"shares"`
	}

	t.Run("decodes JSON into the target type", func(t *testing.T) {
		out, err := ParseLogOutputAsType[sample](`{"staker":"0xabc","shares":"100"}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out.Staker != "0xabc" {
			t.Errorf("Staker = %q, want %q", out.Staker, "0xabc")
		}
		if out.Shares.String() != "100" {
			t.Errorf("Shares = %q, want %q", out.Shares.String(), "100")
		}
	})

	// UseNumber() must be applied so that share values larger than 2^53 (where a float64
	// would silently lose precision) survive the round-trip exactly.
	t.Run("preserves precision of large numeric values", func(t *testing.T) {
		const bigShares = "115792089237316195423570985008687907853269984665640564039457584007913129639935"
		out, err := ParseLogOutputAsType[sample](`{"shares":` + bigShares + `}`)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if out.Shares.String() != bigShares {
			t.Errorf("Shares = %q, want %q", out.Shares.String(), bigShares)
		}
	})

	t.Run("returns an error for malformed JSON", func(t *testing.T) {
		if _, err := ParseLogOutputAsType[sample](`{"staker":`); err == nil {
			t.Error("expected an error for malformed JSON, got nil")
		}
	})
}
