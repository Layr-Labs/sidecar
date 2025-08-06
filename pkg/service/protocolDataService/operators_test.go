package protocolDataService

import (
	"context"
	"strings"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator"
	"github.com/stretchr/testify/assert"
)

func Test_ProtocolDataServiceOperators(t *testing.T) {
	if !tests.LargeTestsEnabled() {
		t.Skipf("Skipping large test")
		return
	}

	grm, l, cfg, err := setup("preprodSlim")

	t.Logf("Using database with name: %s", cfg.DatabaseConfig.DbName)

	if err != nil {
		t.Fatalf("Failed to setup test: %v", err)
	}

	smig, err := stateMigrator.NewStateMigrator(grm, cfg, l)
	if err != nil {
		t.Fatalf("Failed to create state migrator: %v", err)
	}
	sm := stateManager.NewEigenStateManager(smig, l, grm)

	pds := NewProtocolDataService(sm, grm, l, cfg)

	t.Run("Test ListOperatorsForStaker", func(t *testing.T) {
		staker := "0x130c646e1224d979ff23523308abb6012ce04b0a"
		blockNumber := uint64(23075000)

		operators, err := pds.ListOperatorsForStaker(context.Background(), staker, blockNumber)
		assert.Nil(t, err)
		assert.True(t, len(operators) >= 0)

		// Test with 0 block height (current)
		operators2, err := pds.ListOperatorsForStaker(context.Background(), staker, 0)
		assert.Nil(t, err)
		assert.True(t, len(operators2) >= 0)

		// Test case sensitivity
		operatorsUpper, err := pds.ListOperatorsForStaker(context.Background(), strings.ToUpper(staker), blockNumber)
		assert.Nil(t, err)
		assert.Equal(t, operators, operatorsUpper, "Results should be the same regardless of case")
	})

	t.Run("Test ListOperatorsForStrategy", func(t *testing.T) {
		strategy := "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"
		blockNumber := uint64(23075000)

		operators, err := pds.ListOperatorsForStrategy(context.Background(), strategy, blockNumber)
		assert.Nil(t, err)
		assert.True(t, len(operators) >= 0)

		// Verify operators are unique
		uniqueOperators := make(map[string]bool)
		for _, operator := range operators {
			assert.False(t, uniqueOperators[operator], "Operators should be unique")
			uniqueOperators[operator] = true
			assert.True(t, len(operator) > 0, "Operator address should not be empty")
		}

		// Test with 0 block height (current)
		operators2, err := pds.ListOperatorsForStrategy(context.Background(), strategy, 0)
		assert.Nil(t, err)
		assert.True(t, len(operators2) >= 0)

		// Test case sensitivity
		operatorsUpper, err := pds.ListOperatorsForStrategy(context.Background(), strings.ToUpper(strategy), blockNumber)
		assert.Nil(t, err)
		assert.Equal(t, operators, operatorsUpper, "Results should be the same regardless of case")
	})

	t.Run("Test ListOperatorsForAvs", func(t *testing.T) {
		avs := "0xd9b1da8159cf83ccc55ad5757bea33e6f0ce34be"
		blockNumber := uint64(23075000)

		operators, err := pds.ListOperatorsForAvs(context.Background(), avs, blockNumber)
		assert.Nil(t, err)
		assert.True(t, len(operators) >= 0, "Should return operators or empty list for this AVS")

		// Verify operators are unique and valid
		uniqueOperators := make(map[string]bool)
		for _, operator := range operators {
			assert.False(t, uniqueOperators[operator], "Operators should be unique")
			uniqueOperators[operator] = true
			assert.True(t, len(operator) > 0, "Operator address should not be empty")
			assert.True(t, strings.HasPrefix(strings.ToLower(operator), "0x"), "Operator should be a valid address")
		}

		// Test with 0 block height (current)
		operators2, err := pds.ListOperatorsForAvs(context.Background(), avs, 0)
		assert.Nil(t, err)
		assert.True(t, len(operators2) >= 0)

		// Test case sensitivity
		operatorsUpper, err := pds.ListOperatorsForAvs(context.Background(), strings.ToUpper(avs), blockNumber)
		assert.Nil(t, err)
		assert.Equal(t, operators, operatorsUpper, "Results should be the same regardless of case")
	})

	t.Run("Test ListOperatorsForBlockRange", func(t *testing.T) {
		startBlock := uint64(23074900)
		endBlock := uint64(23075100)

		t.Run("Test without filters", func(t *testing.T) {
			operators, err := pds.ListOperatorsForBlockRange(context.Background(), startBlock, endBlock, "", "", "")
			assert.Nil(t, err)
			assert.True(t, len(operators) >= 0, "Should return operators or empty list")

			// Verify operators are unique
			uniqueOperators := make(map[string]bool)
			for _, operator := range operators {
				assert.False(t, uniqueOperators[operator], "Operators should be unique")
				uniqueOperators[operator] = true
				assert.True(t, len(operator) > 0, "Operator address should not be empty")
			}
		})

		t.Run("Test with AVS filter", func(t *testing.T) {
			avs := "0xd9b1da8159cf83ccc55ad5757bea33e6f0ce34be"
			operators, err := pds.ListOperatorsForBlockRange(context.Background(), startBlock, endBlock, avs, "", "")
			assert.Nil(t, err)
			assert.True(t, len(operators) >= 0, "Should return filtered operators or empty list")
		})

		t.Run("Test with strategy filter", func(t *testing.T) {
			strategy := "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"
			operators, err := pds.ListOperatorsForBlockRange(context.Background(), startBlock, endBlock, "", strategy, "")
			assert.Nil(t, err)
			assert.True(t, len(operators) >= 0, "Should return filtered operators or empty list")
		})

		t.Run("Test with staker filter", func(t *testing.T) {
			staker := "0x130c646e1224d979ff23523308abb6012ce04b0a"
			operators, err := pds.ListOperatorsForBlockRange(context.Background(), startBlock, endBlock, "", "", staker)
			assert.Nil(t, err)
			assert.True(t, len(operators) >= 0, "Should return filtered operators or empty list")
		})

		t.Run("Test single block range", func(t *testing.T) {
			singleBlock := uint64(23075000)
			operators, err := pds.ListOperatorsForBlockRange(context.Background(), singleBlock, singleBlock, "", "", "")
			assert.Nil(t, err)
			assert.True(t, len(operators) >= 0, "Should work for single block range")
		})
	})
}
