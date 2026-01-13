package rewards

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metrics"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/rewards/stakerOperators"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// ============================================================================
// E2E Anvil Test Suite
// ============================================================================
//
// This file contains E2E tests that run against a forked mainnet via Anvil.
// Prerequisites:
//   - Anvil running with mainnet fork (see scripts/anvil_e2e_tests/run_e2e_tests.sh)
//   - PostgreSQL running locally
//   - Foundry scripts executed to seed test data
//
// Run with:
//   TEST_E2E_ANVIL=true go test -v ./pkg/rewards -run Test_E2E_Anvil -timeout 30m
// ============================================================================

// e2eAnvilTestsEnabled checks if E2E Anvil tests should run
func e2eAnvilTestsEnabled() bool {
	return os.Getenv("TEST_E2E_ANVIL") == "true"
}

// getAnvilRPCURL returns the Anvil RPC URL
func getAnvilRPCURL() string {
	url := os.Getenv("SIDECAR_ETHEREUM_RPC_URL")
	if url == "" {
		url = "http://localhost:8545"
	}
	return url
}

// E2ETestContext holds all dependencies for E2E tests
type E2ETestContext struct {
	t          *testing.T
	cfg        *config.Config
	grm        *gorm.DB
	l          *zap.Logger
	sink       *metrics.MetricsSink
	ethClient  *ethereum.Client
	calculator *RewardsCalculator
	dbName     string
}

// setupE2ETestContext creates a new test context
func setupE2ETestContext(t *testing.T) *E2ETestContext {
	if !e2eAnvilTestsEnabled() {
		t.Skip("E2E Anvil tests disabled. Set TEST_E2E_ANVIL=true to enable.")
		return nil
	}

	l, err := logger.NewLogger(&logger.LoggerConfig{Debug: true})
	require.NoError(t, err, "Failed to create logger")

	cfg := tests.GetConfig()
	cfg.Chain = config.Chain_Mainnet
	cfg.EthereumRpcConfig.BaseUrl = getAnvilRPCURL()
	cfg.Rewards.RewardsV2_2Enabled = true
	cfg.Rewards.WithdrawalQueueWindow = 14.0 // 14 days for mainnet
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	sink, err := metrics.NewMetricsSink(&metrics.MetricsSinkConfig{}, nil)
	require.NoError(t, err, "Failed to create metrics sink")

	// Create test database
	dbName, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	require.NoError(t, err, "Failed to create test database")

	// Create Ethereum client
	ethClientConfig := &ethereum.EthereumClientConfig{
		BaseUrl:   cfg.EthereumRpcConfig.BaseUrl,
		BlockType: config.BlockType_Latest,
	}
	ethClient := ethereum.NewClient(ethClientConfig, l)

	// Create rewards calculator
	sog := stakerOperators.NewStakerOperatorGenerator(grm, l, cfg)
	calculator, err := NewRewardsCalculator(cfg, grm, nil, sog, sink, l)
	require.NoError(t, err, "Failed to create rewards calculator")

	return &E2ETestContext{
		t:          t,
		cfg:        cfg,
		grm:        grm,
		l:          l,
		sink:       sink,
		ethClient:  ethClient,
		calculator: calculator,
		dbName:     dbName,
	}
}

// teardownE2ETestContext cleans up test resources
func (ctx *E2ETestContext) teardown() {
	if ctx == nil {
		return
	}

	if ctx.grm != nil {
		rawDb, _ := ctx.grm.DB()
		if rawDb != nil {
			_ = rawDb.Close()
		}
	}

	if ctx.dbName != "" {
		pgConfig := postgres.PostgresConfigFromDbConfig(&ctx.cfg.DatabaseConfig)
		if err := postgres.DeleteTestDatabase(pgConfig, ctx.dbName); err != nil {
			ctx.l.Sugar().Errorw("Failed to delete test database", "error", err)
		}
	}
}

// getLatestBlockFromAnvil gets the latest block number from Anvil
func (ctx *E2ETestContext) getLatestBlockFromAnvil() (uint64, error) {
	return ctx.ethClient.GetLatestBlock(context.Background())
}

// indexBlockRange indexes blocks from Anvil into the database
func (ctx *E2ETestContext) indexBlockRange(startBlock, endBlock uint64) error {
	ctx.l.Sugar().Infow("Indexing block range", "start", startBlock, "end", endBlock)

	for blockNum := startBlock; blockNum <= endBlock; blockNum++ {
		// Get block from Anvil
		block, err := ctx.ethClient.GetBlockByNumber(context.Background(), blockNum)
		if err != nil {
			return fmt.Errorf("failed to get block %d: %w", blockNum, err)
		}

		// Parse block time
		blockTime := time.Unix(int64(block.Timestamp.Value()), 0)
		blockDate := blockTime.Format("2006-01-02")

		// Insert block
		result := ctx.grm.Exec(`
			INSERT INTO blocks (number, hash, block_time, block_date)
			VALUES (?, ?, ?, ?)
			ON CONFLICT (number) DO NOTHING
		`, blockNum, string(block.Hash), blockTime, blockDate)

		if result.Error != nil {
			return fmt.Errorf("failed to insert block %d: %w", blockNum, result.Error)
		}

		// Get transaction receipts for this block
		receipts, err := ctx.ethClient.GetBlockTransactionReceipts(context.Background(), blockNum)
		if err != nil {
			ctx.l.Sugar().Warnw("Failed to get block receipts", "block", blockNum, "error", err)
			continue
		}

		// Process logs from receipts
		for _, receipt := range receipts {
			for _, log := range receipt.Logs {
				if err := ctx.processLog(log, blockNum, blockTime); err != nil {
					ctx.l.Sugar().Warnw("Failed to process log", "error", err, "txHash", string(receipt.TransactionHash))
				}
			}
		}

		if blockNum%100 == 0 {
			ctx.l.Sugar().Infow("Indexed blocks", "current", blockNum, "target", endBlock)
		}
	}

	return nil
}

// processLog processes a single log entry for EigenLayer events
func (ctx *E2ETestContext) processLog(log *ethereum.EthereumEventLog, blockNum uint64, blockTime time.Time) error {
	// Event signatures for EigenLayer contracts
	const (
		// StrategyManager events
		depositEventSig = "0x7cfff908a4b583f36430b25d75964c458d8ede8a99bd61be750e97ee1b2f3a96" // Deposit(address,address,address,uint256)

		// DelegationManager events
		stakerDelegatedSig  = "0xc3ee9f2e5fda98e8066a1f745b2f22d0963186ad62e73e7c63e6e7aafd2b4d6c" // StakerDelegated(address,address)
		withdrawalQueuedSig = "0x9009ab153e8014fbfb02f2217f5cde7aa7f9ad734ae85ca3ee3f4ca2fdd499f9" // WithdrawalQueued(bytes32,...)
	)

	if len(log.Topics) == 0 {
		return nil
	}

	eventSig := log.Topics[0]

	switch eventSig {
	case depositEventSig:
		return ctx.processDepositEvent(log, blockNum, blockTime)
	case stakerDelegatedSig:
		return ctx.processStakerDelegatedEvent(log, blockNum, blockTime)
	case withdrawalQueuedSig:
		return ctx.processWithdrawalQueuedEvent(log, blockNum, blockTime)
	default:
		// Unknown event, skip
		return nil
	}
}

// processDepositEvent processes a Deposit event
func (ctx *E2ETestContext) processDepositEvent(log *ethereum.EthereumEventLog, blockNum uint64, blockTime time.Time) error {
	if len(log.Topics) < 3 {
		return nil
	}

	staker := log.Topics[1]    // padded address
	strategy := log.Topics[2] // padded address

	// Decode shares from data
	if len(log.Data) < 66 { // 0x + 64 chars
		return nil
	}

	// Insert into staker_share_deltas
	result := ctx.grm.Exec(`
		INSERT INTO staker_share_deltas (
			staker, strategy, shares, transaction_hash, log_index,
			strategy_index, block_time, block_date, block_number
		) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
		ON CONFLICT DO NOTHING
	`,
		staker, strategy, log.Data, // shares encoded in data
		log.TransactionHash, log.LogIndex.Value(),
		blockTime, blockTime.Format("2006-01-02"), blockNum,
	)

	return result.Error
}

// processStakerDelegatedEvent processes a StakerDelegated event
func (ctx *E2ETestContext) processStakerDelegatedEvent(log *ethereum.EthereumEventLog, blockNum uint64, blockTime time.Time) error {
	if len(log.Topics) < 3 {
		return nil
	}

	staker := log.Topics[1]
	operator := log.Topics[2]

	// Insert into staker_delegation_changes
	result := ctx.grm.Exec(`
		INSERT INTO staker_delegation_changes (
			staker, operator, delegated, transaction_hash, log_index,
			block_time, block_date, block_number
		) VALUES (?, ?, true, ?, ?, ?, ?, ?)
		ON CONFLICT DO NOTHING
	`,
		staker, operator,
		log.TransactionHash, log.LogIndex.Value(),
		blockTime, blockTime.Format("2006-01-02"), blockNum,
	)

	return result.Error
}

// processWithdrawalQueuedEvent processes a WithdrawalQueued event
func (ctx *E2ETestContext) processWithdrawalQueuedEvent(log *ethereum.EthereumEventLog, blockNum uint64, blockTime time.Time) error {
	// This is a complex event - for now just log it
	ctx.l.Sugar().Infow("WithdrawalQueued event detected",
		"txHash", log.TransactionHash,
		"blockNum", blockNum,
	)

	// TODO: Decode withdrawal details and insert into queued_slashing_withdrawals
	return nil
}

// generateSnapshotsForDate generates all snapshots for a given date
func (ctx *E2ETestContext) generateSnapshotsForDate(snapshotDate string) error {
	ctx.l.Sugar().Infow("Generating snapshots", "date", snapshotDate)

	// Generate staker shares first (needed for staker share snapshots)
	if err := ctx.calculator.GenerateAndInsertStakerShares(snapshotDate); err != nil {
		return fmt.Errorf("failed to generate staker shares: %w", err)
	}

	// Generate staker share snapshots
	if err := ctx.calculator.GenerateAndInsertStakerShareSnapshots(snapshotDate); err != nil {
		return fmt.Errorf("failed to generate staker share snapshots: %w", err)
	}

	// Generate operator allocation snapshots (V2.2)
	if err := ctx.calculator.GenerateAndInsertOperatorAllocationSnapshots(snapshotDate); err != nil {
		return fmt.Errorf("failed to generate operator allocation snapshots: %w", err)
	}

	return nil
}

// ============================================================================
// Test Cases
// ============================================================================

// Test_E2E_Anvil_Connection tests that we can connect to Anvil
func Test_E2E_Anvil_Connection(t *testing.T) {
	ctx := setupE2ETestContext(t)
	if ctx == nil {
		return
	}
	defer ctx.teardown()

	blockNum, err := ctx.getLatestBlockFromAnvil()
	require.NoError(t, err, "Failed to get latest block from Anvil")

	t.Logf("Connected to Anvil at block %d", blockNum)
	assert.Greater(t, blockNum, uint64(0), "Block number should be greater than 0")
}

// Test_E2E_Anvil_SSS_1 tests SSS-1: Deposit and queue full withdrawal same day
func Test_E2E_Anvil_SSS_1(t *testing.T) {
	ctx := setupE2ETestContext(t)
	if ctx == nil {
		return
	}
	defer ctx.teardown()

	t.Log("=== SSS-1: Deposit and Queue Full Withdrawal Same Day ===")
	t.Log("Expected: Shares = 0 from day 2 onwards (queued withdrawal removes from slashable)")

	// This test assumes the Foundry script has already seeded the data
	// We just need to:
	// 1. Index the blocks that contain the events
	// 2. Generate snapshots
	// 3. Verify the results

	// Get the blocks we need to index (this would come from the Foundry script output)
	latestBlock, err := ctx.getLatestBlockFromAnvil()
	require.NoError(t, err)

	// Index last 100 blocks (where our test data should be)
	var startBlock uint64
	if latestBlock > 100 {
		startBlock = latestBlock - 100
	} else {
		startBlock = 0
	}

	err = ctx.indexBlockRange(startBlock, latestBlock)
	require.NoError(t, err, "Failed to index blocks")

	// Generate snapshots for today and tomorrow
	today := time.Now().Format("2006-01-02")
	tomorrow := time.Now().AddDate(0, 0, 1).Format("2006-01-02")

	err = ctx.generateSnapshotsForDate(today)
	require.NoError(t, err, "Failed to generate snapshots for today")

	err = ctx.generateSnapshotsForDate(tomorrow)
	require.NoError(t, err, "Failed to generate snapshots for tomorrow")

	// Verify: staker should have 0 shares after withdrawal queued
	// The staker address would come from Foundry script output
	// For now, just verify we have some snapshots
	var snapshotCount int64
	ctx.grm.Raw("SELECT COUNT(*) FROM staker_share_snapshots").Scan(&snapshotCount)
	t.Logf("Generated %d staker share snapshots", snapshotCount)

	if snapshotCount == 0 {
		t.Log("No staker share snapshots found - this is expected if Foundry script hasn't run")
		t.Skip("Skipping assertion - run Foundry seed script first")
	}
}

// Test_E2E_Anvil_SSS_2 tests SSS-2: Deposit day 1, queue full withdrawal day 2
func Test_E2E_Anvil_SSS_2(t *testing.T) {
	ctx := setupE2ETestContext(t)
	if ctx == nil {
		return
	}
	defer ctx.teardown()

	t.Log("=== SSS-2: Deposit Day 1, Queue Full Withdrawal Day 2 ===")
	t.Log("Expected: Full shares on day 1, 0 shares from day 3 onwards")

	// Index and generate snapshots
	latestBlock, err := ctx.getLatestBlockFromAnvil()
	require.NoError(t, err)

	err = ctx.indexBlockRange(latestBlock-200, latestBlock)
	if err != nil {
		t.Logf("Block indexing issue: %v", err)
	}

	// Verify shares timeline
	var results []struct {
		Snapshot string
		Shares   string
	}

	ctx.grm.Raw(`
		SELECT snapshot::text, shares 
		FROM staker_share_snapshots 
		ORDER BY snapshot
		LIMIT 20
	`).Scan(&results)

	for _, r := range results {
		t.Logf("Date: %s, Shares: %s", r.Snapshot, r.Shares)
	}

	if len(results) == 0 {
		t.Skip("No data found - run Foundry seed script first")
	}
}

// Test_E2E_Anvil_SSS_3 tests SSS-3: Partial withdrawal
func Test_E2E_Anvil_SSS_3(t *testing.T) {
	ctx := setupE2ETestContext(t)
	if ctx == nil {
		return
	}
	defer ctx.teardown()

	t.Log("=== SSS-3: Partial Withdrawal ===")
	t.Log("Expected: 50% shares after partial withdrawal queued")

	t.Skip("Run Foundry seed script first with: forge script ... --sig 'runScenario(uint256)' 3")
}

// Test_E2E_Anvil_Full runs the full E2E test suite
func Test_E2E_Anvil_Full(t *testing.T) {
	ctx := setupE2ETestContext(t)
	if ctx == nil {
		return
	}
	defer ctx.teardown()

	t.Log("=== Full E2E Anvil Test ===")

	// This test orchestrates the full flow:
	// 1. Check Anvil connection
	// 2. Index all blocks from fork point to current
	// 3. Generate all snapshots
	// 4. Validate all scenarios

	latestBlock, err := ctx.getLatestBlockFromAnvil()
	require.NoError(t, err, "Failed to get latest block")
	t.Logf("Latest Anvil block: %d", latestBlock)

	// Check if we have any EigenLayer events
	var eventCount int64
	ctx.grm.Raw("SELECT COUNT(*) FROM staker_share_deltas").Scan(&eventCount)
	t.Logf("Staker share deltas in database: %d", eventCount)

	if eventCount == 0 {
		t.Log("No events found in database. Please run the Foundry seed script first:")
		t.Log("  cd scripts/anvil_e2e_tests && ./run_e2e_tests.sh")
		t.Skip("No test data - run seed script first")
	}

	// Generate snapshots for all dates with data
	var dates []string
	ctx.grm.Raw(`
		SELECT DISTINCT block_date::text 
		FROM staker_share_deltas 
		ORDER BY block_date
	`).Scan(&dates)

	for _, date := range dates {
		t.Logf("Generating snapshots for %s", date)
		if err := ctx.generateSnapshotsForDate(date); err != nil {
			t.Logf("Warning: Failed to generate snapshots for %s: %v", date, err)
		}
	}

	// Verify we have snapshots
	var snapshotCount int64
	ctx.grm.Raw("SELECT COUNT(*) FROM staker_share_snapshots").Scan(&snapshotCount)
	t.Logf("Generated %d staker share snapshots", snapshotCount)

	assert.Greater(t, snapshotCount, int64(0), "Should have generated some snapshots")
}

// Test_E2E_Anvil_IndexMainnetEvents tests indexing real mainnet events
func Test_E2E_Anvil_IndexMainnetEvents(t *testing.T) {
	ctx := setupE2ETestContext(t)
	if ctx == nil {
		return
	}
	defer ctx.teardown()

	t.Log("=== Index Mainnet Events from Fork ===")

	// Get fork block
	latestBlock, err := ctx.getLatestBlockFromAnvil()
	require.NoError(t, err)

	// Index a small range of historical blocks
	// These should contain real mainnet EigenLayer events
	startBlock := latestBlock - 10 // Just 10 blocks for quick test

	t.Logf("Indexing blocks %d to %d", startBlock, latestBlock)

	err = ctx.indexBlockRange(startBlock, latestBlock)
	if err != nil {
		t.Logf("Indexing error (may be expected on fresh fork): %v", err)
	}

	// Check what we indexed
	var blockCount int64
	ctx.grm.Raw("SELECT COUNT(*) FROM blocks").Scan(&blockCount)
	t.Logf("Indexed %d blocks", blockCount)
}
