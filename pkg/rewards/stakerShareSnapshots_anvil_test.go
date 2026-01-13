package rewards

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// Test_SSS_Anvil_E2E tests Staker Share Snapshots E2E scenarios (SSS-8 through SSS-15)
// These tests use real contract interactions via Anvil to verify the full flow:
// contract events → indexer → database → snapshot generation
//
// Prerequisites:
//  1. Anvil running: anvil --fork-url $RPC_URL --fork-block-number $BLOCK
//  2. Contracts deployed or forked
//  3. Environment variables set:
//     - TEST_ANVIL=true
//     - ANVIL_RPC_URL (default: http://localhost:8545)
//     - REWARDS_COORDINATOR_ADDRESS
//     - DELEGATION_MANAGER_ADDRESS
//     - ALLOCATION_MANAGER_ADDRESS
//     - STRATEGY_MANAGER_ADDRESS
//     - TEST_PRIVATE_KEY (optional, uses Anvil default account)
func Test_SSS_Anvil_E2E(t *testing.T) {
	// Check if Anvil tests are enabled
	if os.Getenv("TEST_ANVIL") != "true" {
		t.Skip("Skipping Anvil E2E tests. Set TEST_ANVIL=true to run")
	}

	anvilURL := getEnvOrDefault("ANVIL_RPC_URL", "http://localhost:8545")

	// Connect to Anvil
	client, err := ethclient.Dial(anvilURL)
	if err != nil {
		t.Fatalf("Failed to connect to Anvil at %s: %v\nIs Anvil running?", anvilURL, err)
	}
	defer client.Close()

	ctx := context.Background()

	// Verify connection
	chainID, err := client.ChainID(ctx)
	require.NoError(t, err, "Failed to get chain ID")
	t.Logf("✓ Connected to Anvil at %s (Chain ID: %s)", anvilURL, chainID.String())

	// Setup test account
	privateKey, testAccount := setupSSS_TestAccount(t)
	t.Logf("✓ Using test account: %s", testAccount.Hex())

	// Create transactor
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainID)
	require.NoError(t, err, "Failed to create transactor")

	// Get contract addresses
	strategyManagerAddr := getSSS_RequiredEnv(t, "STRATEGY_MANAGER_ADDRESS")
	delegationManagerAddr := getSSS_RequiredEnv(t, "DELEGATION_MANAGER_ADDRESS")
	allocationManagerAddr := getSSS_RequiredEnv(t, "ALLOCATION_MANAGER_ADDRESS")

	t.Logf("✓ StrategyManager: %s", strategyManagerAddr)
	t.Logf("✓ DelegationManager: %s", delegationManagerAddr)
	t.Logf("✓ AllocationManager: %s", allocationManagerAddr)

	// Create snapshot before tests (for cleanup)
	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err, "Failed to create initial snapshot")
	t.Logf("✓ Created Anvil snapshot: %s", snapshotID)

	// Setup sidecar components
	dbFileName, cfg, grm, l := setupSSS_SidecarForAnvil(t, anvilURL)
	t.Logf("✓ Sidecar database: %s", dbFileName)

	// Cleanup function
	defer func() {
		t.Log("\n========== CLEANUP ==========")

		// Revert Anvil state
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		} else {
			t.Log("✓ Reverted Anvil to initial snapshot")
		}

		// Clean database
		cleanupIntegrationTestData(t, grm)

		// Remove test database file
		if err := os.Remove(dbFileName); err != nil {
			t.Logf("Warning: Failed to remove test database: %v", err)
		} else {
			t.Logf("✓ Removed test database: %s", dbFileName)
		}

		t.Log("========== CLEANUP COMPLETE ==========\n")
	}()

	// Initialize rewards calculator
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	rc, err := NewRewardsCalculator(cfg, grm, nil, sog, nil, l)
	require.NoError(t, err, "Failed to create RewardsCalculator")

	// Run SSS E2E test scenarios
	t.Run("SSS-8_E2E: Multiple consecutive slashes", func(t *testing.T) {
		testSSS8_MultipleConsecutiveSlashes(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-9_E2E: Partial withdrawal with slashing (delegated)", func(t *testing.T) {
		testSSS9_PartialWithdrawalWithSlashing(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-10_E2E: Slash during withdrawal queue", func(t *testing.T) {
		testSSS10_SlashDuringWithdrawalQueue(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-11_E2E: Slash after withdrawal block boundary", func(t *testing.T) {
		testSSS11_SlashAfterWithdrawalBoundary(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-12_E2E: Dual slashing - operator set and beacon chain", func(t *testing.T) {
		testSSS12_DualSlashing(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-13_E2E: Same block - withdrawal before slash", func(t *testing.T) {
		testSSS13_SameBlockWithdrawalBeforeSlash(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-14_E2E: Same block - slash before withdrawal", func(t *testing.T) {
		testSSS14_SameBlockSlashBeforeWithdrawal(t, ctx, client, auth, grm, rc, l)
	})

	t.Run("SSS-15_E2E: Multiple deposits then withdrawal", func(t *testing.T) {
		testSSS15_MultipleDepositsThenWithdrawal(t, ctx, client, auth, grm, rc, l)
	})
}

// SSS-8: Multiple consecutive slashes
// Actions:
// 1. Staker delegates to operator
// 2. Staker deposits 200 shares on 1/4 @ 5pm
// 3. Staker queues a partial withdrawal for 50 shares on 1/5 @ 6pm
// 4. Staker is slashed 50% on 1/6 @ 6pm
// 5. Staker is slashed another 50% on 1/7 @ 6pm
//
// Expected:
// - Full shares (200) till 1/6
// - Half shares (100) starting 1/7
// - Quarter shares (50) starting 1/8
// - queued_withdrawal_slashing_adjustments: 1.0 → 0.5 → 0.25
func testSSS8_MultipleConsecutiveSlashes(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	// Snapshot for cleanup
	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("SSS-8: Setting up test scenario...")

	// TODO: Implement contract interactions
	// 1. Deploy mock strategy
	// 2. Register operator to operator set
	// 3. Delegate staker to operator
	// 4. Deposit 200 shares on 1/4 @ 5pm
	// 5. Queue 50 shares withdrawal on 1/5 @ 6pm
	// 6. Index events - verify queued_withdrawal_slashing_adjustments created with multiplier 1.0
	// 7. Slash operator 50% on 1/6 @ 6pm
	// 8. Index events - verify multiplier updated to 0.5
	// 9. Slash operator 50% again on 1/7 @ 6pm
	// 10. Index events - verify multiplier updated to 0.25
	// 11. Generate snapshots
	// 12. Verify: 200 till 1/6, 100 at 1/7, 50 at 1/8

	t.Skip("SSS-8 E2E test implementation pending - requires contract bindings and event indexing")
}

// SSS-9: Partial withdrawal with slashing (delegated)
// Same as SSS-8 but with explicit delegation/registration setup
// Verifies contract interactions populate queued_withdrawal_slashing_adjustments correctly
func testSSS9_PartialWithdrawalWithSlashing(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("SSS-9: Setting up test scenario...")

	// TODO: Implement contract interactions
	// Same as SSS-8 but with explicit delegation/registration verification

	t.Skip("SSS-9 E2E test implementation pending - requires contract bindings and event indexing")
}

// SSS-10: Slash during withdrawal queue (precise timing)
// Actions:
// 1. Staker delegates to operator. Operator registers to set and allocates
// 2. Staker deposits 1000 shares on 1/4 @ 5pm
// 3. Staker queues a partial withdrawal (250 shares) on 1/5 @ 6pm at block 100,000
// 4. Staker is slashed 50% on 1/6 @ 6pm
// 5. On 1/19 at block 100k + 14 days, the staker is slashed another 50%
//
// Expected:
// - Full shares (1000) till 1/6
// - Half shares (500) starting 1/7
// - On 1/20 should have 187.5 shares (slash affects queued withdrawal)
func testSSS10_SlashDuringWithdrawalQueue(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("SSS-10: Setting up test scenario...")

	// TODO: Implement contract interactions with time manipulation
	// 1. Setup and deposit 1000 shares
	// 2. Queue 250 shares withdrawal
	// 3. Index: verify queued_withdrawal_slashing_adjustments created
	// 4. Slash operator 50% at day 6
	// 5. Advance time to day 19, slash operator 50% again
	// 6. Generate snapshots
	// 7. Verify snapshots: 1000 till day 6, 500 at day 7, 187.5 at day 20

	t.Skip("SSS-10 E2E test implementation pending - requires contract bindings and time manipulation")
}

// SSS-11: Slash after withdrawal block boundary
// Same as SSS-10 but last slash does not affect the withdrawal
// Expected: 187.5 shares on 1/20, withdrawal can be completed for 125 shares
func testSSS11_SlashAfterWithdrawalBoundary(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("SSS-11: Setting up test scenario...")

	// TODO: Implement contract interactions
	// Same as SSS-10 but last slash at block 100k + 14 days + 1

	t.Skip("SSS-11 E2E test implementation pending - requires contract bindings and time manipulation")
}

// SSS-12: Dual slashing - operator set and beacon chain
// Actions:
// 1. Staker delegates to operator. Operator registers to set and allocates
// 2. Staker deposits 200 shares on 1/4 @ 5pm
// 3. Staker queues a withdrawal for 50 shares on 1/5 @ 6pm at block 100,000
// 4. Staker is slashed by operatorSet by 25% on 1/6 @ 6pm
// 5. Staker is slashed by beacon chain (BeaconChainSlashingFactorDecreased event) by 50% on 1/7 @ 6pm
//
// Expected:
// - Full till 1/6
// - 150 shares on 1/7
// - 75 shares on 1/8
// - 56.25 shares on 1/20
func testSSS12_DualSlashing(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("SSS-12: Setting up test scenario...")

	// TODO: Implement contract interactions with both operator set and beacon chain slashing
	// 1. Setup and deposit 200 shares
	// 2. Queue 50 shares withdrawal
	// 3. Slash by operator set 25%
	// 4. Slash by beacon chain 50%
	// 5. Generate snapshots
	// 6. Verify: Full till 1/6, 150 on 1/7, 75 on 1/8, 56.25 on 1/20

	t.Skip("SSS-12 E2E test implementation pending - requires contract bindings for dual slashing sources")
}

// SSS-13: Same block events - withdrawal before slash
// Actions:
// 1. Staker delegates to operator. Operator registers to set and allocates
// 2. Staker deposits 1000 shares on 1/4 @ 5pm
// 3. Staker queues a full withdrawal on 1/5 @ 6pm. Block 1000. Log index 2
// 4. Staker is slashed immediately in the same block but at a later log index (log 3)
//
// Expected:
// - Full shares till 1/5
// - Half shares starting 1/6
// - Verify log_index ordering in queued_withdrawal_slashing_adjustments
func testSSS13_SameBlockWithdrawalBeforeSlash(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("SSS-13: Setting up test scenario...")

	// TODO: Implement contract interactions in same block
	// 1. Setup and deposit 1000 shares
	// 2. In SAME BLOCK: queue withdrawal (log 2), then slash (log 3)
	// 3. Index: verify log_index ordering in queued_withdrawal_slashing_adjustments
	// 4. Verify: slash affects the queued withdrawal

	t.Skip("SSS-13 E2E test implementation pending - requires same-block transaction orchestration")
}

// SSS-14: Same block events - slash before withdrawal
// Actions:
// 1. Staker delegates to operator. Operator registers to set and allocates
// 2. Staker deposits 1000 shares on 1/4 @ 5pm
// 3. Staker is slashed 50% on 1/5 @ 6pm. Block 1000. Log index 2
// 4. Staker queues a full withdrawal on 1/5 @ 6pm. Block 1000. Log index 3
//
// Expected:
// - Full shares till 1/5
// - Half shares starting 1/6
// - Staker has no shares by 1/20
// - Verify queued withdrawal reflects post-slash amount
func testSSS14_SameBlockSlashBeforeWithdrawal(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("SSS-14: Setting up test scenario...")

	// TODO: Implement contract interactions in same block
	// 1. Setup and deposit 1000 shares
	// 2. In SAME BLOCK: slash (log 2), then queue withdrawal (log 3)
	// 3. Index: verify queued shares are already slashed
	// 4. Verify: withdrawal amount reflects post-slash shares

	t.Skip("SSS-14 E2E test implementation pending - requires same-block transaction orchestration")
}

// SSS-15: Multiple deposits then withdrawal
// Actions:
// 1. Staker delegates to operator. Operator registers to set and allocates
// 2. Staker deposits 100 shares on 1/4 @ 5pm
// 3. Staker deposits 50 shares on 1/5 @ 5pm
// 4. Staker queues a withdrawal for 10 shares on 1/5 @ 5pm
//
// Expected:
// - 100 shares on 1/5
// - 150 shares starting 1/6
// - 140 shares starting 1/20 (after withdrawal completes)
func testSSS15_MultipleDepositsThenWithdrawal(t *testing.T, ctx context.Context,
	client *ethclient.Client, auth *bind.TransactOpts,
	grm *gorm.DB, rc *RewardsCalculator, l *zap.Logger) {

	snapshotID, err := anvilSnapshot(ctx, client)
	require.NoError(t, err)
	defer func() {
		if err := anvilRevert(ctx, client, snapshotID); err != nil {
			t.Logf("Warning: Failed to revert Anvil snapshot: %v", err)
		}
	}()

	t.Log("SSS-15: Setting up test scenario...")

	// TODO: Implement contract interactions
	// 1. Setup and deposit 100 shares
	// 2. Deposit 50 more shares
	// 3. Queue 10 shares withdrawal
	// 4. Generate snapshots
	// 5. Verify: 100 on 1/5, 150 starting 1/6, 140 starting 1/20

	t.Skip("SSS-15 E2E test implementation pending - requires contract bindings and event indexing")
}

// Helper function to setup test account
func setupSSS_TestAccount(t *testing.T) (*ecdsa.PrivateKey, common.Address) {
	t.Helper()

	// Try to get private key from environment
	privateKeyHex := os.Getenv("TEST_PRIVATE_KEY")
	if privateKeyHex == "" {
		// Use Anvil default account #0
		// Private key: 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
		privateKeyHex = "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
	}

	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	require.NoError(t, err, "Failed to parse private key")

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	require.True(t, ok, "Failed to cast public key to ECDSA")

	address := crypto.PubkeyToAddress(*publicKeyECDSA)
	return privateKey, address
}

// Helper function to get required environment variable
func getSSS_RequiredEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("Required environment variable %s is not set", key)
	}
	return value
}

// Helper function to setup sidecar for Anvil tests
func setupSSS_SidecarForAnvil(t *testing.T, anvilURL string) (string, *config.Config, *gorm.DB, *zap.Logger) {
	t.Helper()

	cfg := config.NewConfig()
	cfg.Chain = config.Chain_PreprodHoodi
	cfg.Debug = true
	cfg.DatabaseConfig.Host = "localhost"
	cfg.DatabaseConfig.Port = 5432
	cfg.DatabaseConfig.DbName = fmt.Sprintf("test_sss_anvil_%d", time.Now().Unix())
	cfg.DatabaseConfig.User = os.Getenv("POSTGRES_USER")
	cfg.DatabaseConfig.Password = os.Getenv("POSTGRES_PASSWORD")
	cfg.Rewards.WithdrawalQueueWindow = 14 // 14 days

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	require.NoError(t, err, "Failed to create test database")

	return dbname, cfg, grm, l
}
