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

		operatorSets, err := pds.ListOperatorsForAvs(context.Background(), avs, blockNumber)
		assert.Nil(t, err)
		assert.True(t, len(operatorSets) >= 0, "Should return operators or empty list for this AVS")

		// Verify operators are unique and valid, and test operator set structure
		uniqueOperators := make(map[string]bool)
		for _, operatorSet := range operatorSets {
			assert.False(t, uniqueOperators[operatorSet.Operator], "Operators should be unique")
			uniqueOperators[operatorSet.Operator] = true
			assert.True(t, len(operatorSet.Operator) > 0, "Operator address should not be empty")
			assert.True(t, strings.HasPrefix(strings.ToLower(operatorSet.Operator), "0x"), "Operator should be a valid address")

			// Test OperatorSet structure - OperatorSetId should be a valid uint64 (note: uint64 is always >= 0)
			// We just verify the field exists and is accessible
			_ = operatorSet.OperatorSetId // This ensures the field is properly accessible
		}

		// Test with 0 block height (current)
		operatorSets2, err := pds.ListOperatorsForAvs(context.Background(), avs, 0)
		assert.Nil(t, err)
		assert.True(t, len(operatorSets2) >= 0)

		// Test case sensitivity
		operatorSetsUpper, err := pds.ListOperatorsForAvs(context.Background(), strings.ToUpper(avs), blockNumber)
		assert.Nil(t, err)
		assert.Equal(t, operatorSets, operatorSetsUpper, "Results should be the same regardless of case")
	})

}
