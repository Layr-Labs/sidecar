package contractAbi

import (
	"github.com/ethereum/go-ethereum/accounts/abi"
	"go.uber.org/zap"
	"regexp"
)

// UnmarshalJsonToAbi unmarshals a JSON ABI string into an abi.ABI struct.
// It handles certain common unmarshaling errors that can be safely ignored,
// such as "only single receive is allowed" and "only single fallback is allowed".
// Returns the parsed ABI and any error encountered during parsing.
func UnmarshalJsonToAbi(json string, l *zap.Logger) (*abi.ABI, error) {
	a := &abi.ABI{}

	err := a.UnmarshalJSON([]byte(json))

	if err != nil {
		foundMatch := false
		// patterns that we're fine to ignore and not treat as an error
		patterns := []*regexp.Regexp{
			regexp.MustCompile(`only single receive is allowed`),
			regexp.MustCompile(`only single fallback is allowed`),
		}

		for _, pattern := range patterns {
			if pattern.MatchString(err.Error()) {
				foundMatch = true
				break
			}
		}

		// If the error isnt one that we can ignore, return it
		if !foundMatch {
			l.Sugar().Warnw("Error unmarshaling abi json", zap.Error(err))
			return nil, err
		}
	}

	return a, nil
}
