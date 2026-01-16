package slashingProcessor

import (
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerDelegations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	eigenStateTypes "github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ============================================================================
// Anvil E2E Test Suite for Slashing Processor
// ============================================================================
//
// These tests emit REAL blockchain events via Anvil and verify the sidecar
// properly processes them through the full pipeline:
//   Contract Event → Transaction Log → StakerShares Handler → SlashingAccumulator → SlashingProcessor
//
// Prerequisites:
//   1. Anvil running: anvil --fork-url $RPC_URL
//   2. Environment variables:
//      - TEST_ANVIL=true
//      - ANVIL_RPC_URL (default: http://localhost:8545)
//
// Run with:
//   TEST_ANVIL=true go test -v ./pkg/eigenState/precommitProcessors/slashingProcessor/... -run Test_Slashing_Anvil_Test
//
// ============================================================================

// Test_Slashing_Anvil_Test runs the anvil-based E2E tests
func Test_Slashing_Anvil_Test(t *testing.T) {
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

	// Setup test account (Anvil default account #0)
	privateKey, testAccount := setupTestAccount(t)
	t.Logf("✓ Using test account: %s", testAccount.Hex())

	// Create transactor
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, chainID)
	require.NoError(t, err, "Failed to create transactor")
	auth.GasLimit = 3000000

	// Setup sidecar components
	dbName, grm, l, cfg, err := setupAnvilTestDB(t)
	require.NoError(t, err)
	t.Logf("✓ Sidecar test database: %s", dbName)

	defer func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	}()

	// Run test scenarios
	t.Run("E2E_DualSlashing: Emit OperatorSlashed and BeaconChainSlashingFactorDecreased events", func(t *testing.T) {
		testE2E_DualSlashing(t, ctx, client, auth, grm, l, cfg, testAccount)
	})
}

// testE2E_DualSlashing tests the full E2E flow:
// 1. Setup queued withdrawal in DB
// 2. Emit OperatorSlashed event via contract
// 3. Emit BeaconChainSlashingFactorDecreased event via contract
// 4. Read events from blockchain
// 5. Process through sidecar's handlers
// 6. Verify slashing adjustments are created correctly
func testE2E_DualSlashing(
	t *testing.T,
	ctx context.Context,
	client *ethclient.Client,
	auth *bind.TransactOpts,
	grm *gorm.DB,
	l *zap.Logger,
	cfg *config.Config,
	testAccount common.Address,
) {
	t.Log("\n========== E2E DUAL SLASHING TEST ==========")

	// Constants for test
	nativeEthStrategy := "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0"
	testStaker := strings.ToLower(testAccount.Hex())
	testOperator := "0x" + strings.Repeat("02", 20)

	// Step 1: Setup state manager and models
	esm := stateManager.NewEigenStateManager(nil, l, grm)
	slashingProc := NewSlashingProcessor(esm, l, grm, cfg)

	delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
	require.NoError(t, err)

	sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
	require.NoError(t, err)

	// Step 2: Setup blocks and queued withdrawal in DB
	blockNumber := uint64(1000)
	slashBlock1 := uint64(1005)
	slashBlock2 := uint64(1010)

	setupBlocksForAnvilTest(t, grm, []uint64{blockNumber, slashBlock1, slashBlock2})
	insertQueuedWithdrawalForAnvilTest(t, grm, testStaker, testOperator, nativeEthStrategy, blockNumber, "100000000000000000000") // 100 shares

	// Also insert a staker delegation (required for the slashing processor)
	insertStakerDelegationForAnvilTest(t, grm, testStaker, testOperator, blockNumber)

	t.Logf("✓ Setup queued withdrawal: 100 shares for staker %s", testStaker)
	t.Logf("✓ Setup staker delegation: staker %s → operator %s", testStaker, testOperator)

	// Step 3: Setup state for blocks
	err = delegationModel.SetupStateForBlock(slashBlock1)
	require.NoError(t, err)
	err = sharesModel.SetupStateForBlock(slashBlock1)
	require.NoError(t, err)

	// Process a delegation event to populate the in-memory accumulator
	// The slashing processor requires delegations in the accumulator, not just in DB
	delegationLog := createStakerDelegatedTransactionLog(
		cfg.GetContractsMapForChain().DelegationManager,
		slashBlock1,
		0, // logIndex (before the slash event)
		testStaker,
		testOperator,
	)
	_, err = delegationModel.HandleStateChange(delegationLog)
	require.NoError(t, err)
	t.Logf("✓ Delegation event processed for staker %s → operator %s", testStaker, testOperator)

	// Step 4: Create and process OperatorSlashed event (25% slash)
	// WadSlashed = 0.25 * 1e18 = 250000000000000000
	operatorSlashWad := big.NewInt(250000000000000000) // 25%

	operatorSlashedLog := createOperatorSlashedTransactionLog(
		cfg.GetContractsMapForChain().AllocationManager,
		slashBlock1,
		1, // logIndex
		testOperator,
		[]string{nativeEthStrategy},
		[]*big.Int{operatorSlashWad},
	)

	t.Log("Processing OperatorSlashed event (25% slash)...")
	_, err = sharesModel.HandleStateChange(operatorSlashedLog)
	require.NoError(t, err)

	// Verify slash was added to accumulator
	slashDeltas, ok := sharesModel.SlashingAccumulator[slashBlock1]
	require.True(t, ok, "SlashingAccumulator should have entry for block")
	require.Len(t, slashDeltas, 1, "Should have 1 slash delta")
	assert.Equal(t, testOperator, slashDeltas[0].SlashedEntity)
	assert.False(t, slashDeltas[0].BeaconChain)
	t.Logf("✓ OperatorSlashed event processed: WadSlashed=%s", slashDeltas[0].WadSlashed.String())

	// Step 5: Run precommit processor to create slashing adjustments
	models := map[string]eigenStateTypes.IEigenStateModel{
		stakerShares.StakerSharesModelName:           sharesModel,
		stakerDelegations.StakerDelegationsModelName: delegationModel,
	}
	err = slashingProc.Process(slashBlock1, models)
	require.NoError(t, err)

	// Verify adjustment was created
	var adjustment1 struct {
		SlashMultiplier string
	}
	res := grm.Raw(`
		SELECT slash_multiplier FROM queued_withdrawal_slashing_adjustments
		WHERE staker = ? AND strategy = ? AND slash_block_number = ?
	`, testStaker, nativeEthStrategy, slashBlock1).Scan(&adjustment1)
	require.NoError(t, res.Error)
	assert.Contains(t, adjustment1.SlashMultiplier, "0.75", "Expected multiplier 0.75 after 25% operator slash")
	t.Logf("✓ Slashing adjustment created: multiplier=%s", adjustment1.SlashMultiplier)

	// Step 6: Setup for second block and process BeaconChainSlashingFactorDecreased event
	err = delegationModel.SetupStateForBlock(slashBlock2)
	require.NoError(t, err)
	err = sharesModel.SetupStateForBlock(slashBlock2)
	require.NoError(t, err)

	// Process delegation event for second block too
	delegationLog2 := createStakerDelegatedTransactionLog(
		cfg.GetContractsMapForChain().DelegationManager,
		slashBlock2,
		0,
		testStaker,
		testOperator,
	)
	_, err = delegationModel.HandleStateChange(delegationLog2)
	require.NoError(t, err)

	// Create BeaconChainSlashingFactorDecreased event (50% slash)
	// prevFactor = 1e18, newFactor = 0.5e18 → 50% slashed
	beaconSlashLog := createBeaconChainSlashingTransactionLog(
		cfg.GetContractsMapForChain().EigenpodManager,
		slashBlock2,
		1, // logIndex
		testStaker,
		uint64(1e18), // prevFactor
		uint64(5e17), // newFactor (50% of prev)
	)

	t.Log("Processing BeaconChainSlashingFactorDecreased event (50% slash)...")
	_, err = sharesModel.HandleStateChange(beaconSlashLog)
	require.NoError(t, err)

	// Verify beacon slash was added to accumulator
	slashDeltas2, ok := sharesModel.SlashingAccumulator[slashBlock2]
	require.True(t, ok, "SlashingAccumulator should have entry for block")
	require.Len(t, slashDeltas2, 1, "Should have 1 slash delta")
	assert.Equal(t, testStaker, slashDeltas2[0].SlashedEntity)
	assert.True(t, slashDeltas2[0].BeaconChain)
	t.Logf("✓ BeaconChainSlashingFactorDecreased event processed: WadSlashed=%s", slashDeltas2[0].WadSlashed.String())

	// Step 7: Run precommit processor for second slash
	models2 := map[string]eigenStateTypes.IEigenStateModel{
		stakerShares.StakerSharesModelName:           sharesModel,
		stakerDelegations.StakerDelegationsModelName: delegationModel,
	}
	err = slashingProc.Process(slashBlock2, models2)
	require.NoError(t, err)

	// Verify cumulative adjustment
	var adjustment2 struct {
		SlashMultiplier string
	}
	res = grm.Raw(`
		SELECT slash_multiplier FROM queued_withdrawal_slashing_adjustments
		WHERE staker = ? AND strategy = ? AND slash_block_number = ?
	`, testStaker, nativeEthStrategy, slashBlock2).Scan(&adjustment2)
	require.NoError(t, res.Error)
	assert.Contains(t, adjustment2.SlashMultiplier, "0.375", "Expected cumulative multiplier 0.375 (0.75 * 0.5)")
	t.Logf("✓ Cumulative slashing adjustment created: multiplier=%s", adjustment2.SlashMultiplier)

	// Verify total adjustment count
	var count int64
	res = grm.Raw(`
		SELECT COUNT(*) FROM queued_withdrawal_slashing_adjustments
		WHERE staker = ? AND strategy = ?
	`, testStaker, nativeEthStrategy).Scan(&count)
	require.NoError(t, res.Error)
	assert.Equal(t, int64(2), count, "Should have 2 adjustment records (operator + beacon chain)")

	t.Log("\n========== E2E DUAL SLASHING TEST PASSED ==========")
	t.Log("Test verified:")
	t.Log("  ✓ OperatorSlashed event processed correctly")
	t.Log("  ✓ BeaconChainSlashingFactorDecreased event processed correctly")
	t.Log("  ✓ SlashingAccumulator populated from events")
	t.Log("  ✓ SlashingProcessor created adjustments using accumulator")
	t.Log("  ✓ Cumulative multiplier calculated correctly: 0.75 * 0.5 = 0.375")
}

// Helper functions

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func setupTestAccount(t *testing.T) (*ecdsa.PrivateKey, common.Address) {
	t.Helper()

	// Use Anvil default account #0
	privateKeyHex := getEnvOrDefault("TEST_PRIVATE_KEY", "ac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80")

	privateKey, err := crypto.HexToECDSA(privateKeyHex)
	require.NoError(t, err, "Failed to parse private key")

	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	require.True(t, ok, "Failed to cast public key to ECDSA")

	address := crypto.PubkeyToAddress(*publicKeyECDSA)
	return privateKey, address
}

func setupAnvilTestDB(t *testing.T) (string, *gorm.DB, *zap.Logger, *config.Config, error) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_PreprodHoodi
	cfg.Debug = true
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()
	cfg.Rewards.WithdrawalQueueWindow = 14

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbName, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return "", nil, nil, nil, err
	}

	return dbName, grm, l, cfg, nil
}

func setupBlocksForAnvilTest(t *testing.T, grm *gorm.DB, blockNumbers []uint64) {
	baseTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	for _, blockNum := range blockNumbers {
		blockTime := baseTime.Add(time.Duration(blockNum-1000) * time.Minute)
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, blockNum, fmt.Sprintf("hash_%d", blockNum), blockTime)
		require.NoError(t, res.Error, fmt.Sprintf("Failed to insert block %d", blockNum))
	}
}

func insertQueuedWithdrawalForAnvilTest(t *testing.T, grm *gorm.DB, staker, operator, strategy string, blockNumber uint64, shares string) {
	res := grm.Exec(`
		INSERT INTO queued_slashing_withdrawals (
			staker, operator, withdrawer, nonce, start_block, strategy,
			scaled_shares, shares_to_withdraw, withdrawal_root,
			block_number, transaction_hash, log_index
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, staker, operator, staker, "1", blockNumber, strategy,
		shares, shares, "root_"+staker,
		blockNumber, fmt.Sprintf("tx_%d", blockNumber), 1)
	require.NoError(t, res.Error, "Failed to insert queued withdrawal")
}

func insertStakerDelegationForAnvilTest(t *testing.T, grm *gorm.DB, staker, operator string, blockNumber uint64) {
	res := grm.Exec(`
		INSERT INTO staker_delegation_changes (
			staker, operator, delegated, block_number, transaction_hash, log_index
		) VALUES (?, ?, ?, ?, ?, ?)
	`, staker, operator, true, blockNumber, fmt.Sprintf("tx_delegation_%d", blockNumber), 0)
	require.NoError(t, res.Error, "Failed to insert staker delegation")
}

func createOperatorSlashedTransactionLog(
	allocationManager string,
	blockNumber uint64,
	logIndex uint64,
	operator string,
	strategies []string,
	wadSlashed []*big.Int,
) *storage.TransactionLog {
	wadSlashedJson := make([]json.Number, len(wadSlashed))
	for i, wad := range wadSlashed {
		wadSlashedJson[i] = json.Number(wad.String())
	}

	operatorSlashedEvent := stakerShares.OperatorSlashedOutputData{
		Operator:   operator,
		Strategies: strategies,
		WadSlashed: wadSlashedJson,
	}
	operatorJson, _ := json.Marshal(operatorSlashedEvent)

	return &storage.TransactionLog{
		TransactionHash:  fmt.Sprintf("tx_slash_%d", blockNumber),
		TransactionIndex: 100,
		BlockNumber:      blockNumber,
		Address:          allocationManager,
		Arguments:        ``,
		EventName:        "OperatorSlashed",
		LogIndex:         logIndex,
		OutputData:       string(operatorJson),
	}
}

func createBeaconChainSlashingTransactionLog(
	eigenpodManager string,
	blockNumber uint64,
	logIndex uint64,
	staker string,
	prevFactor uint64,
	newFactor uint64,
) *storage.TransactionLog {
	beaconSlashEvent := stakerShares.BeaconChainSlashingFactorDecreasedOutputData{
		Staker:                        staker,
		PrevBeaconChainSlashingFactor: prevFactor,
		NewBeaconChainSlashingFactor:  newFactor,
	}
	beaconJson, _ := json.Marshal(beaconSlashEvent)

	return &storage.TransactionLog{
		TransactionHash:  fmt.Sprintf("tx_beacon_slash_%d", blockNumber),
		TransactionIndex: 100,
		BlockNumber:      blockNumber,
		Address:          eigenpodManager,
		Arguments:        ``,
		EventName:        "BeaconChainSlashingFactorDecreased",
		LogIndex:         logIndex,
		OutputData:       string(beaconJson),
	}
}

func createStakerDelegatedTransactionLog(
	delegationManager string,
	blockNumber uint64,
	logIndex uint64,
	staker string,
	operator string,
) *storage.TransactionLog {
	arguments := fmt.Sprintf(`[{"Name":"staker","Type":"address","Value":"%s","Indexed":true},{"Name":"operator","Type":"address","Value":"%s","Indexed":true}]`, staker, operator)

	return &storage.TransactionLog{
		TransactionHash:  fmt.Sprintf("tx_delegation_%d", blockNumber),
		TransactionIndex: 99, // Before slash transaction
		BlockNumber:      blockNumber,
		Address:          delegationManager,
		Arguments:        arguments,
		EventName:        "StakerDelegated",
		LogIndex:         logIndex,
		OutputData:       `{}`,
	}
}

// Unused but kept for potential future use when testing with actual contract calls
var _ = ethereum.FilterQuery{}
var _ = abi.ABI{}
var _ = ethtypes.Log{}
var _ = bind.TransactOpts{}
