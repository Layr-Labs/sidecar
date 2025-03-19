package protocolDataService

import (
	"context"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ProtocolDataServiceStrategies(t *testing.T) {
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

	t.Run("Test ListStrategies - for all strategies", func(t *testing.T) {
		strategies, err := pds.ListStrategies(context.Background(), []string{}, 2964996)
		assert.Nil(t, err)
		assert.Equal(t, 38, len(strategies))
	})
	t.Run("Test ListStrategies - for specified strategies", func(t *testing.T) {
		strategy := "0x1bc0b67cccd43aaeb380c56a3a17200f3d1cf81b"
		strategies, err := pds.ListStrategies(context.Background(), []string{strategy}, 2964996)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(strategies))
		assert.Equal(t, 3, len(strategies[0].RewardTokens))
		assert.Equal(t, "22357468030049875087", strategies[0].TotalStaked)
	})
	t.Run("Test ListStakerStrategies", func(t *testing.T) {
		staker := "0x6f774586d4269ec8f92ce935004673a4be634cc4"
		strategies, err := pds.ListStakerStrategies(context.Background(), staker, 2964996, nil)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(strategies))
	})
	t.Run("Test GetStrategyForStaker", func(t *testing.T) {
		staker := "0x6f774586d4269ec8f92ce935004673a4be634cc4"
		strategy := "0x24da526f9e465c4fb6bae41e226df8aa5b34eac7"

		stakerStrategy, err := pds.GetStrategyForStaker(context.Background(), staker, strategy, 2964996)
		assert.Nil(t, err)
		assert.NotNil(t, stakerStrategy)
	})
}
