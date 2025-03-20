package protocolDataService

import (
	"context"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_ProtocolDataServiceQueuedWithdrawals(t *testing.T) {
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

	t.Run("Test ListQueuedWithdrawals for staker", func(t *testing.T) {
		staker := "0x6f774586d4269ec8f92ce935004673a4be634cc4"

		withdrawals, err := pds.ListQueuedWithdrawals(context.Background(), WithdrawalFilters{Staker: staker}, 2964996)
		assert.Nil(t, err)
		assert.Equal(t, 9, len(withdrawals))
	})
	t.Run("Test ListQueuedWithdrawals for strategy", func(t *testing.T) {
		strategy := "0x24da526f9e465c4fb6bae41e226df8aa5b34eac7"

		withdrawals, err := pds.ListQueuedWithdrawals(context.Background(), WithdrawalFilters{Strategies: []string{strategy}}, 2971378)
		assert.Nil(t, err)
		assert.Equal(t, 5, len(withdrawals))
	})

	t.Run("Test ListOperatorQueuedWithdrawals", func(t *testing.T) {
		operator := "0x5e90ef3a3cf11e59e2e750955edf4cf4bd862cdb"

		withdrawals, err := pds.ListOperatorQueuedWithdrawals(context.Background(), operator, 2971378)
		assert.Nil(t, err)
		assert.Equal(t, 5, len(withdrawals))
	})

	t.Run("Test ListOperatorQueuedWithdrawalsForStrategy", func(t *testing.T) {
		operator := "0x5e90ef3a3cf11e59e2e750955edf4cf4bd862cdb"
		strategy := "0x24da526f9e465c4fb6bae41e226df8aa5b34eac7"

		withdrawal, err := pds.ListOperatorQueuedWithdrawalsForStrategy(context.Background(), operator, strategy, 2971378)
		assert.Nil(t, err)
		assert.NotNil(t, withdrawal)
	})
}
