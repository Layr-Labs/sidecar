package slashingProcessor

import (
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/queuedSlashingWithdrawals"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerDelegations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup() (
	string,
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	error,
) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Mainnet
	cfg.Debug = false
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func withSlashingProcessor(esm *stateManager.EigenStateManager, grm *gorm.DB, l *zap.Logger, cfg *config.Config) *SlashingProcessor {
	return NewSlashingProcessor(esm, l, grm, cfg)
}

func teardown(db *gorm.DB) {
	queries := []string{
		`truncate table staker_share_deltas cascade`,
		`truncate table blocks cascade`,
		`truncate table transactions cascade`,
		`truncate table transaction_logs cascade`,
		`truncate table staker_delegation_changes cascade`,
	}
	for _, query := range queries {
		db.Exec(query)
	}
}

func Test_SlashingPrecommitProcessor(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should capture delegate, deposit, slash in same block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		withSlashingProcessor(esm, grm, l, cfg)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// ----------------------
		// Handle events
		// ----------------------

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)

		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		diffs := change.(*stakerShares.AccumulatedStateChanges)
		assert.Equal(t, 1, len(diffs.SlashDiffs))

		slashDiff := diffs.SlashDiffs[0]
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.SlashedEntity)
		assert.False(t, slashDiff.BeaconChain)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadSlashed.String())

		err = esm.RunPrecommitProcessors(blockNumber)
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `
			select * from staker_share_deltas
			where block_number = ?
			order by log_index asc
		`
		results := []*stakerShares.StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)
		assert.Equal(t, 2, len(results))

		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "1000000000000000000", results[0].Shares)
		assert.Equal(t, uint64(400), results[0].LogIndex)

		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[1].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[1].Strategy)
		assert.Equal(t, "-100000000000000000", results[1].Shares)

		teardown(grm)
	})

	t.Run("Should capture many deposits and slash in same block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		withSlashingProcessor(esm, grm, l, cfg)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// ----------------------
		// Handle events
		// ----------------------
		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)
		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 301, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)
		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 401, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(2e18))
		assert.Nil(t, err)

		_, err = processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		// -----------------------
		// pre-commit and then commit
		// -----------------------
		err = esm.RunPrecommitProcessors(blockNumber)
		assert.Nil(t, err)
		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `
			select * from staker_share_deltas
			where block_number = ?
			order by log_index, staker asc
		`
		results := []*stakerShares.StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 4, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "1000000000000000000", results[0].Shares)

		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", results[1].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[1].Strategy)
		assert.Equal(t, "2000000000000000000", results[1].Shares)

		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[2].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[2].Strategy)
		assert.Equal(t, "-100000000000000000", results[2].Shares)

		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", results[3].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[3].Strategy)
		assert.Equal(t, "-200000000000000000", results[3].Shares)

		teardown(grm)
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}

func createBlock(db *gorm.DB, blockNumber uint64) error {
	block := storage.Block{
		Number: blockNumber,
		Hash:   "some hash",
	}
	res := db.Model(storage.Block{}).Create(&block)
	if res.Error != nil {
		return res.Error
	}

	return nil
}

func processDelegation(delegationModel *stakerDelegations.StakerDelegationsModel, delegationManager string, blockNumber, logIndex uint64, staker, operator string) (interface{}, error) {
	delegateLog := storage.TransactionLog{
		TransactionHash:  "some hash",
		TransactionIndex: 100,
		BlockNumber:      blockNumber,
		Address:          delegationManager,
		Arguments:        fmt.Sprintf(`[{"Name":"staker","Type":"address","Value":"%s","Indexed":true},{"Name":"operator","Type":"address","Value":"%s","Indexed":true}]`, staker, operator),
		EventName:        "StakerDelegated",
		LogIndex:         logIndex,
		OutputData:       `{}`,
		CreatedAt:        time.Time{},
		UpdatedAt:        time.Time{},
		DeletedAt:        time.Time{},
	}

	return delegationModel.HandleStateChange(&delegateLog)
}

func processDeposit(stakerSharesModel *stakerShares.StakerSharesModel, strategyManager string, blockNumber, logIndex uint64, staker, strategy string, shares *big.Int) (interface{}, error) {
	depositLog := storage.TransactionLog{
		TransactionHash:  "some hash",
		TransactionIndex: 100,
		BlockNumber:      blockNumber,
		Address:          strategyManager,
		Arguments:        `[{"Name": "staker", "Type": "address", "Value": ""}, {"Name": "token", "Type": "address", "Value": ""}, {"Name": "strategy", "Type": "address", "Value": ""}, {"Name": "shares", "Type": "uint256", "Value": ""}]`,
		EventName:        "Deposit",
		LogIndex:         logIndex,
		OutputData:       fmt.Sprintf(`{"token": "%s", "shares": %s, "staker": "%s", "strategy": "%s"}`, strategy, shares.String(), staker, strategy),
		CreatedAt:        time.Time{},
		UpdatedAt:        time.Time{},
		DeletedAt:        time.Time{},
	}

	return stakerSharesModel.HandleStateChange(&depositLog)
}

func processSlashing(stakerSharesModel *stakerShares.StakerSharesModel, allocationManager string, blockNumber, logIndex uint64, operator string, strategies []string, wadSlashed []*big.Int) (interface{}, error) {
	wadSlashedJson := make([]json.Number, len(wadSlashed))
	for i, wad := range wadSlashed {
		wadSlashedJson[i] = json.Number(wad.String())
	}

	operatorSlashedEvent := stakerShares.OperatorSlashedOutputData{
		Operator:   operator,
		Strategies: strategies,
		WadSlashed: wadSlashedJson,
	}
	operatorJson, err := json.Marshal(operatorSlashedEvent)
	if err != nil {
		return nil, err
	}

	slashingLog := storage.TransactionLog{
		TransactionHash:  "some hash",
		TransactionIndex: 100,
		BlockNumber:      blockNumber,
		Address:          allocationManager,
		Arguments:        ``,
		EventName:        "OperatorSlashed",
		LogIndex:         logIndex,
		OutputData:       string(operatorJson),
		CreatedAt:        time.Time{},
		UpdatedAt:        time.Time{},
		DeletedAt:        time.Time{},
	}

	return stakerSharesModel.HandleStateChange(&slashingLog)
}

// ============================================================================
// CreateSlashingAdjustments Tests
// ============================================================================

// Test_CreateSlashingAdjustments tests the createSlashingAdjustments function
// which creates adjustment records for queued withdrawals when an operator is slashed.
func Test_CreateSlashingAdjustments(t *testing.T) {
	dbName, grm, l, cfg, err := setupCSATest()
	require.NoError(t, err, "Failed to setup test")

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})

	// CSA-1: Single slash adjustment
	t.Run("CSA-1: Single slash adjustment", func(t *testing.T) {
		cleanupCSATest(t, grm)

		// Setup: Staker queues withdrawal on block 1000
		setupBlocksForCSA(t, grm, []uint64{1000, 1005})
		insertQueuedWithdrawal(t, grm, "0xstaker1", "0xoperator1", "0xstrategy1", 1000, "1000000000000000000000")

		// Set up staker shares model to process slashing event
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)
		err = sharesModel.SetupStateForBlock(1005)
		require.NoError(t, err)

		// Create processor and call createSlashingAdjustments
		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// Process slashing through the staker shares model to get proper SlashDiff
		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, 1005, 1, "0xoperator1", []string{"0xstrategy1"}, []*big.Int{big.NewInt(25e16)})
		require.NoError(t, err)

		diffs := change.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs.SlashDiffs), "Should have one slash diff")

		slashDiff := diffs.SlashDiffs[0]
		slashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff.SlashedEntity,
			BeaconChain:     slashDiff.BeaconChain,
			Strategy:        slashDiff.Strategy,
			WadSlashed:      slashDiff.WadSlashed.String(),
			TransactionHash: slashDiff.TransactionHash,
			LogIndex:        slashDiff.LogIndex,
		}

		err = sp.createSlashingAdjustments(slashEvent, 1005, nil)
		require.NoError(t, err)

		// Verify adjustment record created with multiplier 0.75
		var adjustment struct {
			Staker                string
			Strategy              string
			Operator              string
			WithdrawalBlockNumber uint64
			SlashBlockNumber      uint64
			SlashMultiplier       string
		}
		res := grm.Raw(`
			SELECT staker, strategy, operator, withdrawal_block_number, slash_block_number, slash_multiplier
			FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ? AND operator = ?
		`, "0xstaker1", "0xstrategy1", "0xoperator1").Scan(&adjustment)
		require.NoError(t, res.Error)

		assert.Equal(t, "0xstaker1", adjustment.Staker)
		assert.Equal(t, "0xstrategy1", adjustment.Strategy)
		assert.Equal(t, "0xoperator1", adjustment.Operator)
		assert.Equal(t, uint64(1000), adjustment.WithdrawalBlockNumber)
		assert.Equal(t, uint64(1005), adjustment.SlashBlockNumber)
		// PostgreSQL NUMERIC returns values with trailing zeros
		assert.Contains(t, adjustment.SlashMultiplier, "0.75", "Expected multiplier 0.75 (1 - 0.25)")
	})

	// CSA-2: Cumulative slashing (multiple slashes, compound multiplier)
	t.Run("CSA-2: Cumulative slashing", func(t *testing.T) {
		cleanupCSATest(t, grm)

		// Setup: Staker queues withdrawal on block 1000
		setupBlocksForCSA(t, grm, []uint64{1000, 1005, 1010})
		insertQueuedWithdrawal(t, grm, "0xstaker2", "0xoperator2", "0xstrategy2", 1000, "1000000000000000000000")

		// Set up staker shares model to process slashing events
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)

		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// First slash: 25% at block 1005
		err = sharesModel.SetupStateForBlock(1005)
		require.NoError(t, err)
		change1, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, 1005, 1, "0xoperator2", []string{"0xstrategy2"}, []*big.Int{big.NewInt(25e16)})
		require.NoError(t, err)
		diffs1 := change1.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs1.SlashDiffs))
		slashDiff1 := diffs1.SlashDiffs[0]
		slashEvent1 := &SlashingEvent{
			SlashedEntity:   slashDiff1.SlashedEntity,
			BeaconChain:     slashDiff1.BeaconChain,
			Strategy:        slashDiff1.Strategy,
			WadSlashed:      slashDiff1.WadSlashed.String(),
			TransactionHash: slashDiff1.TransactionHash,
			LogIndex:        slashDiff1.LogIndex,
		}
		err = sp.createSlashingAdjustments(slashEvent1, 1005, nil)
		require.NoError(t, err)

		// Verify first adjustment: 0.75
		var multiplier string
		res := grm.Raw(`
			SELECT slash_multiplier FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ? AND slash_block_number = ?
		`, "0xstaker2", "0xstrategy2", 1005).Scan(&multiplier)
		require.NoError(t, res.Error)
		assert.Contains(t, multiplier, "0.75")

		// Second slash: 50% at block 1010
		err = sharesModel.SetupStateForBlock(1010)
		require.NoError(t, err)
		change2, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, 1010, 1, "0xoperator2", []string{"0xstrategy2"}, []*big.Int{big.NewInt(5e17)})
		require.NoError(t, err)
		diffs2 := change2.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs2.SlashDiffs))
		slashDiff2 := diffs2.SlashDiffs[0]
		slashEvent2 := &SlashingEvent{
			SlashedEntity:   slashDiff2.SlashedEntity,
			BeaconChain:     slashDiff2.BeaconChain,
			Strategy:        slashDiff2.Strategy,
			WadSlashed:      slashDiff2.WadSlashed.String(),
			TransactionHash: slashDiff2.TransactionHash,
			LogIndex:        slashDiff2.LogIndex,
		}
		err = sp.createSlashingAdjustments(slashEvent2, 1010, nil)
		require.NoError(t, err)

		// Verify cumulative multiplier: 0.75 * 0.5 = 0.375
		res = grm.Raw(`
			SELECT slash_multiplier FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ? AND slash_block_number = ?
		`, "0xstaker2", "0xstrategy2", 1010).Scan(&multiplier)
		require.NoError(t, res.Error)
		assert.Contains(t, multiplier, "0.375", "Expected cumulative multiplier 0.375 (0.75 * 0.5)")
	})

	// CSA-3: Same-block event ordering (log_index precedence)
	t.Run("CSA-3: Same-block event ordering", func(t *testing.T) {
		cleanupCSATest(t, grm)

		// Setup: Staker queues withdrawal on block 1000, log_index 2
		// Operator slashed on same block 1000, log_index 1 (earlier)
		setupBlocksForCSA(t, grm, []uint64{1000})
		insertQueuedWithdrawalWithLogIndex(t, grm, "0xstaker3", "0xoperator3", "0xstrategy3", 1000, 2, "1000000000000000000000")

		// Set up staker shares model to process slashing event
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)
		err = sharesModel.SetupStateForBlock(1000)
		require.NoError(t, err)

		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// Process slashing at log_index 1 (before withdrawal at log_index 2)
		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, 1000, 1, "0xoperator3", []string{"0xstrategy3"}, []*big.Int{big.NewInt(25e16)})
		require.NoError(t, err)
		diffs := change.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs.SlashDiffs))
		slashDiff := diffs.SlashDiffs[0]
		slashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff.SlashedEntity,
			BeaconChain:     slashDiff.BeaconChain,
			Strategy:        slashDiff.Strategy,
			WadSlashed:      slashDiff.WadSlashed.String(),
			TransactionHash: slashDiff.TransactionHash,
			LogIndex:        slashDiff.LogIndex,
		}

		err = sp.createSlashingAdjustments(slashEvent, 1000, nil)
		require.NoError(t, err)

		// Verify NO adjustment created (slash before withdrawal in execution order)
		var count int64
		res := grm.Raw(`
			SELECT COUNT(*) FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ?
		`, "0xstaker3", "0xstrategy3").Scan(&count)
		require.NoError(t, res.Error)
		assert.Equal(t, int64(0), count, "No adjustment should be created when slash occurs before withdrawal in same block")
	})

	// CSA-3b: Same-block event ordering with accumulator (withdrawal before slash)
	// This tests the real E2E flow where withdrawals go to accumulator before DB commit
	t.Run("CSA-3b: Same-block withdrawal before slash via accumulator", func(t *testing.T) {
		cleanupCSATest(t, grm)

		// Setup block
		setupBlocksForCSA(t, grm, []uint64{1000})

		// Set up models
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)
		err = sharesModel.SetupStateForBlock(1000)
		require.NoError(t, err)

		// Create QueuedSlashingWithdrawalModel and set up for block
		qswModel, err := queuedSlashingWithdrawals.NewQueuedSlashingWithdrawalModel(esm, grm, l, cfg)
		require.NoError(t, err)
		err = qswModel.SetupStateForBlock(1000)
		require.NoError(t, err)

		// Process withdrawal through model at log_index 1 (earlier)
		// This puts it in the accumulator, NOT the DB
		withdrawalLog := &storage.TransactionLog{
			TransactionHash:  "0xtx_withdrawal",
			TransactionIndex: 100,
			BlockNumber:      1000,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			EventName:        "SlashingWithdrawalQueued",
			LogIndex:         1,
			OutputData:       `{"withdrawal":{"nonce":"1","staker":"0xstaker3b","startBlock":1000,"strategies":["0xstrategy3b"],"withdrawer":"0xstaker3b","delegatedTo":"0xoperator3b","scaledShares":["1000000000000000000000"]},"withdrawalRoot":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=","sharesToWithdraw":["1000000000000000000000"]}`,
		}
		_, err = qswModel.HandleStateChange(withdrawalLog)
		require.NoError(t, err)

		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// Process slashing at log_index 2 (after withdrawal at log_index 1)
		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, 1000, 2, "0xoperator3b", []string{"0xstrategy3b"}, []*big.Int{big.NewInt(25e16)})
		require.NoError(t, err)
		diffs := change.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs.SlashDiffs))
		slashDiff := diffs.SlashDiffs[0]
		slashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff.SlashedEntity,
			BeaconChain:     slashDiff.BeaconChain,
			Strategy:        slashDiff.Strategy,
			WadSlashed:      slashDiff.WadSlashed.String(),
			TransactionHash: slashDiff.TransactionHash,
			LogIndex:        slashDiff.LogIndex,
		}

		// Pass qswModel so it can find the withdrawal in the accumulator
		err = sp.createSlashingAdjustments(slashEvent, 1000, qswModel)
		require.NoError(t, err)

		// Verify adjustment WAS created (withdrawal happened before slash via accumulator)
		var count int64
		res := grm.Raw(`
			SELECT COUNT(*) FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ?
		`, "0xstaker3b", "0xstrategy3b").Scan(&count)
		require.NoError(t, res.Error)
		assert.Equal(t, int64(1), count, "Adjustment should be created when withdrawal occurs before slash in same block (via accumulator)")

		// Verify multiplier value (0.75 for 25% slash)
		var multiplier string
		res = grm.Raw(`
			SELECT slash_multiplier FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ?
		`, "0xstaker3b", "0xstrategy3b").Scan(&multiplier)
		require.NoError(t, res.Error)
		assert.Contains(t, multiplier, "0.75", "Expected multiplier 0.75 after 25% slash")
	})

	// CSA-4: Expired withdrawal queue (no adjustment after 14 days)
	t.Run("CSA-4: Expired withdrawal queue", func(t *testing.T) {
		cleanupCSATest(t, grm)

		// Setup: Staker queues withdrawal on block 1000 (day 1)
		// Operator slashed on block 1200 (day 20, after 14-day queue expires)
		day1 := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
		day20 := time.Date(2025, 1, 20, 12, 0, 0, 0, time.UTC)

		setupBlocksWithTime(t, grm, []struct {
			number uint64
			time   time.Time
		}{
			{1000, day1},
			{1200, day20},
		})

		insertQueuedWithdrawal(t, grm, "0xstaker4", "0xoperator4", "0xstrategy4", 1000, "1000000000000000000000")

		// Set up staker shares model to process slashing event
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)
		err = sharesModel.SetupStateForBlock(1200)
		require.NoError(t, err)

		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// Process slashing after queue expires
		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, 1200, 1, "0xoperator4", []string{"0xstrategy4"}, []*big.Int{big.NewInt(25e16)})
		require.NoError(t, err)
		diffs := change.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs.SlashDiffs))
		slashDiff := diffs.SlashDiffs[0]
		slashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff.SlashedEntity,
			BeaconChain:     slashDiff.BeaconChain,
			Strategy:        slashDiff.Strategy,
			WadSlashed:      slashDiff.WadSlashed.String(),
			TransactionHash: slashDiff.TransactionHash,
			LogIndex:        slashDiff.LogIndex,
		}

		err = sp.createSlashingAdjustments(slashEvent, 1200, nil)
		require.NoError(t, err)

		// Verify NO adjustment created (withdrawal already completable)
		var count int64
		res := grm.Raw(`
			SELECT COUNT(*) FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ?
		`, "0xstaker4", "0xstrategy4").Scan(&count)
		require.NoError(t, res.Error)
		assert.Equal(t, int64(0), count, "No adjustment should be created after withdrawal queue expires")
	})

	// CSA-5: Multiple stakers affected by single slash
	t.Run("CSA-5: Multiple stakers affected by single slash", func(t *testing.T) {
		cleanupCSATest(t, grm)

		// Setup: Staker1 and Staker2 both queue withdrawals for same operator/strategy
		setupBlocksForCSA(t, grm, []uint64{1000, 1001, 1005, 1006})
		insertQueuedWithdrawal(t, grm, "0xstaker5a", "0xoperator5", "0xstrategy5", 1000, "1000000000000000000000")
		insertQueuedWithdrawal(t, grm, "0xstaker5b", "0xoperator5", "0xstrategy5", 1001, "2000000000000000000000")

		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)
		err = sharesModel.SetupStateForBlock(1005)
		require.NoError(t, err)

		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// Process slashing through the staker shares model to get proper SlashDiff
		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, 1005, 1, "0xoperator5", []string{"0xstrategy5"}, []*big.Int{big.NewInt(3e17)})
		require.NoError(t, err)

		diffs := change.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs.SlashDiffs), "Should have one slash diff")

		slashDiff := diffs.SlashDiffs[0]
		slashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff.SlashedEntity,
			BeaconChain:     slashDiff.BeaconChain,
			Strategy:        slashDiff.Strategy,
			WadSlashed:      slashDiff.WadSlashed.String(),
			TransactionHash: slashDiff.TransactionHash,
			LogIndex:        slashDiff.LogIndex,
		}

		err = sp.createSlashingAdjustments(slashEvent, 1005, nil)
		require.NoError(t, err, "Both stakers should get adjustment records")

		// Verify both stakers got adjustment records
		var count int64
		res := grm.Raw(`
			SELECT COUNT(*) FROM queued_withdrawal_slashing_adjustments
			WHERE operator = ? AND strategy = ?
		`, "0xoperator5", "0xstrategy5").Scan(&count)
		require.NoError(t, res.Error)
		assert.Equal(t, int64(2), count, "Both stakers should have adjustment records")

		// Verify the multiplier is correct for both stakers
		var adjustments []struct {
			Staker          string
			SlashMultiplier string
		}
		res = grm.Raw(`
			SELECT staker, slash_multiplier FROM queued_withdrawal_slashing_adjustments
			WHERE operator = ? AND strategy = ?
			ORDER BY staker
		`, "0xoperator5", "0xstrategy5").Scan(&adjustments)
		require.NoError(t, res.Error)
		require.Equal(t, 2, len(adjustments), "Should have 2 adjustment records")
		assert.Equal(t, "0xstaker5a", adjustments[0].Staker)
		assert.Contains(t, adjustments[0].SlashMultiplier, "0.7", "Expected multiplier 0.7 (1 - 0.3)")
		assert.Equal(t, "0xstaker5b", adjustments[1].Staker)
		assert.Contains(t, adjustments[1].SlashMultiplier, "0.7", "Expected multiplier 0.7 (1 - 0.3)")
	})

	// CSA-6: Multiple withdrawals, partial overlap
	t.Run("CSA-6: Multiple withdrawals, partial overlap", func(t *testing.T) {
		cleanupCSATest(t, grm)

		// Setup: Staker queues 2 separate withdrawals on blocks 1000 and 1010
		// Operator slashed 25% on block 1005
		setupBlocksForCSA(t, grm, []uint64{1000, 1005, 1010})
		insertQueuedWithdrawalWithLogIndex(t, grm, "0xstaker6", "0xoperator6", "0xstrategy6", 1000, 1, "1000000000000000000000")
		insertQueuedWithdrawalWithLogIndex(t, grm, "0xstaker6", "0xoperator6", "0xstrategy6", 1010, 1, "500000000000000000000")

		// Set up staker shares model to process slashing event
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)
		err = sharesModel.SetupStateForBlock(1005)
		require.NoError(t, err)

		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// Process slashing at block 1005
		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, 1005, 1, "0xoperator6", []string{"0xstrategy6"}, []*big.Int{big.NewInt(25e16)})
		require.NoError(t, err)
		diffs := change.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs.SlashDiffs))
		slashDiff := diffs.SlashDiffs[0]
		slashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff.SlashedEntity,
			BeaconChain:     slashDiff.BeaconChain,
			Strategy:        slashDiff.Strategy,
			WadSlashed:      slashDiff.WadSlashed.String(),
			TransactionHash: slashDiff.TransactionHash,
			LogIndex:        slashDiff.LogIndex,
		}

		err = sp.createSlashingAdjustments(slashEvent, 1005, nil)
		require.NoError(t, err)

		// Verify only first withdrawal gets adjustment (second queued after slash)
		var adjustments []struct {
			WithdrawalBlockNumber uint64
			SlashMultiplier       string
		}
		res := grm.Raw(`
			SELECT withdrawal_block_number, slash_multiplier FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ?
			ORDER BY withdrawal_block_number
		`, "0xstaker6", "0xstrategy6").Scan(&adjustments)
		require.NoError(t, res.Error)

		assert.Equal(t, 1, len(adjustments), "Only first withdrawal should have adjustment")
		assert.Equal(t, uint64(1000), adjustments[0].WithdrawalBlockNumber)
		assert.Contains(t, adjustments[0].SlashMultiplier, "0.75")
	})

	// CSA-7: 100% slash edge case
	t.Run("CSA-7: 100% slash edge case", func(t *testing.T) {
		cleanupCSATest(t, grm)

		// Setup: Staker queues withdrawal, operator slashed 100%
		setupBlocksForCSA(t, grm, []uint64{1000, 1005})
		insertQueuedWithdrawal(t, grm, "0xstaker7", "0xoperator7", "0xstrategy7", 1000, "1000000000000000000000")

		// Set up staker shares model to process slashing event
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)
		err = sharesModel.SetupStateForBlock(1005)
		require.NoError(t, err)

		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// Process 100% slash
		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, 1005, 1, "0xoperator7", []string{"0xstrategy7"}, []*big.Int{big.NewInt(1e18)})
		require.NoError(t, err)
		diffs := change.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs.SlashDiffs))
		slashDiff := diffs.SlashDiffs[0]
		slashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff.SlashedEntity,
			BeaconChain:     slashDiff.BeaconChain,
			Strategy:        slashDiff.Strategy,
			WadSlashed:      slashDiff.WadSlashed.String(),
			TransactionHash: slashDiff.TransactionHash,
			LogIndex:        slashDiff.LogIndex,
		}

		err = sp.createSlashingAdjustments(slashEvent, 1005, nil)
		require.NoError(t, err)

		// Verify adjustment record created with multiplier 0
		var multiplier string
		res := grm.Raw(`
			SELECT slash_multiplier FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ?
		`, "0xstaker7", "0xstrategy7").Scan(&multiplier)
		require.NoError(t, res.Error)
		// Check that multiplier is 0 (may have trailing zeros)
		assert.Contains(t, multiplier, "0.0", "Expected multiplier 0 for 100% slash")
	})

	// CSA-8: Strategy isolation
	t.Run("CSA-8: Strategy isolation", func(t *testing.T) {
		cleanupCSATest(t, grm)

		// Setup: Staker queues withdrawal for strategy A
		// Operator slashed on strategy B
		setupBlocksForCSA(t, grm, []uint64{1000, 1005})
		insertQueuedWithdrawal(t, grm, "0xstaker8", "0xoperator8", "0xstrategyA", 1000, "1000000000000000000000")

		// Set up staker shares model to process slashing event
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)
		err = sharesModel.SetupStateForBlock(1005)
		require.NoError(t, err)

		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// Process slash on different strategy (B instead of A)
		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, 1005, 1, "0xoperator8", []string{"0xstrategyB"}, []*big.Int{big.NewInt(25e16)})
		require.NoError(t, err)
		diffs := change.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs.SlashDiffs))
		slashDiff := diffs.SlashDiffs[0]
		slashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff.SlashedEntity,
			BeaconChain:     slashDiff.BeaconChain,
			Strategy:        slashDiff.Strategy,
			WadSlashed:      slashDiff.WadSlashed.String(),
			TransactionHash: slashDiff.TransactionHash,
			LogIndex:        slashDiff.LogIndex,
		}

		err = sp.createSlashingAdjustments(slashEvent, 1005, nil)
		require.NoError(t, err)

		// Verify NO adjustment created (different strategy)
		var count int64
		res := grm.Raw(`
			SELECT COUNT(*) FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ?
		`, "0xstaker8", "0xstrategyA").Scan(&count)
		require.NoError(t, res.Error)
		assert.Equal(t, int64(0), count, "No adjustment should be created for different strategy")
	})

	// CSA-9: Dual slashing (operator + beacon chain)
	// Test scenario:
	// 1. 100 shares in the queue
	// 2. 25% slash on the operator -> multiplier = 0.75
	// 3. 50% slash on beacon chain -> cumulative multiplier = 0.75 * 0.5 = 0.375
	// Final: 100 * 0.375 = 37.5 shares left
	t.Run("CSA-9: Dual slashing (operator + beacon chain)", func(t *testing.T) {
		cleanupCSATest(t, grm)

		// Setup: Staker queues withdrawal on block 1000 with native ETH strategy
		nativeEthStrategy := "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0"
		setupBlocksForCSA(t, grm, []uint64{1000, 1005, 1010})
		insertQueuedWithdrawal(t, grm, "0xstaker9", "0xoperator9", nativeEthStrategy, 1000, "100000000000000000000") // 100 shares

		// Set up staker shares model to process slashing events
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)

		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// First: Operator slash 25% at block 1005
		err = sharesModel.SetupStateForBlock(1005)
		require.NoError(t, err)
		change1, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, 1005, 1, "0xoperator9", []string{nativeEthStrategy}, []*big.Int{big.NewInt(25e16)})
		require.NoError(t, err)
		diffs1 := change1.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs1.SlashDiffs))
		slashDiff1 := diffs1.SlashDiffs[0]
		operatorSlashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff1.SlashedEntity,
			BeaconChain:     slashDiff1.BeaconChain,
			Strategy:        slashDiff1.Strategy,
			WadSlashed:      slashDiff1.WadSlashed.String(),
			TransactionHash: slashDiff1.TransactionHash,
			LogIndex:        slashDiff1.LogIndex,
		}
		err = sp.createSlashingAdjustments(operatorSlashEvent, 1005, nil)
		require.NoError(t, err)

		// Verify first adjustment: multiplier = 0.75 (1 - 0.25)
		var multiplier1 string
		res := grm.Raw(`
			SELECT slash_multiplier FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ? AND slash_block_number = ?
		`, "0xstaker9", nativeEthStrategy, 1005).Scan(&multiplier1)
		require.NoError(t, res.Error)
		assert.Contains(t, multiplier1, "0.75", "Expected multiplier 0.75 after 25% operator slash")

		// Second: Beacon chain slash 50% at block 1010
		// Process through staker shares model
		err = sharesModel.SetupStateForBlock(1010)
		require.NoError(t, err)
		change2, err := processBeaconChainSlashing(sharesModel, cfg.GetContractsMapForChain().EigenpodManager, 1010, 1, "0xstaker9", 1e18, 5e17)
		require.NoError(t, err)
		diffs2 := change2.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs2.SlashDiffs))
		slashDiff2 := diffs2.SlashDiffs[0]
		beaconSlashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff2.SlashedEntity,
			BeaconChain:     slashDiff2.BeaconChain,
			Strategy:        slashDiff2.Strategy,
			WadSlashed:      slashDiff2.WadSlashed.String(),
			TransactionHash: slashDiff2.TransactionHash,
			LogIndex:        slashDiff2.LogIndex,
		}
		err = sp.createSlashingAdjustments(beaconSlashEvent, 1010, nil)
		require.NoError(t, err)

		// Verify cumulative multiplier: 0.75 * 0.5 = 0.375
		var multiplier2 string
		res = grm.Raw(`
			SELECT slash_multiplier FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ? AND slash_block_number = ?
		`, "0xstaker9", nativeEthStrategy, 1010).Scan(&multiplier2)
		require.NoError(t, res.Error)
		assert.Contains(t, multiplier2, "0.375", "Expected cumulative multiplier 0.375 (0.75 * 0.5) after beacon chain slash")

		// Verify total adjustment records
		var count int64
		res = grm.Raw(`
			SELECT COUNT(*) FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ?
		`, "0xstaker9", nativeEthStrategy).Scan(&count)
		require.NoError(t, res.Error)
		assert.Equal(t, int64(2), count, "Should have 2 adjustment records (operator + beacon chain)")
	})

	// CSA-10: Beacon chain slash only (no operator slash)
	t.Run("CSA-10: Beacon chain slash only", func(t *testing.T) {
		cleanupCSATest(t, grm)

		nativeEthStrategy := "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0"
		setupBlocksForCSA(t, grm, []uint64{1000, 1005})
		insertQueuedWithdrawal(t, grm, "0xstaker10", "0xoperator10", nativeEthStrategy, 1000, "100000000000000000000")

		// Set up staker shares model to process slashing event
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)
		err = sharesModel.SetupStateForBlock(1005)
		require.NoError(t, err)

		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// Beacon chain slash 50% - process through staker shares model
		change, err := processBeaconChainSlashing(sharesModel, cfg.GetContractsMapForChain().EigenpodManager, 1005, 1, "0xstaker10", 1e18, 5e17)
		require.NoError(t, err)
		diffs := change.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs.SlashDiffs))
		slashDiff := diffs.SlashDiffs[0]
		beaconSlashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff.SlashedEntity,
			BeaconChain:     slashDiff.BeaconChain,
			Strategy:        slashDiff.Strategy,
			WadSlashed:      slashDiff.WadSlashed.String(),
			TransactionHash: slashDiff.TransactionHash,
			LogIndex:        slashDiff.LogIndex,
		}
		err = sp.createSlashingAdjustments(beaconSlashEvent, 1005, nil)
		require.NoError(t, err)

		// Verify multiplier = 0.5
		var multiplier string
		res := grm.Raw(`
			SELECT slash_multiplier FROM queued_withdrawal_slashing_adjustments
			WHERE staker = ? AND strategy = ?
		`, "0xstaker10", nativeEthStrategy).Scan(&multiplier)
		require.NoError(t, res.Error)
		assert.Contains(t, multiplier, "0.5", "Expected multiplier 0.5 after 50% beacon chain slash")
	})

	// CSA-11: Beacon chain slash doesn't affect other stakers
	t.Run("CSA-11: Beacon chain slash isolation", func(t *testing.T) {
		cleanupCSATest(t, grm)

		nativeEthStrategy := "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0"
		setupBlocksForCSA(t, grm, []uint64{1000, 1001, 1005})
		// Two stakers with same operator
		insertQueuedWithdrawal(t, grm, "0xstaker11a", "0xoperator11", nativeEthStrategy, 1000, "100000000000000000000")
		insertQueuedWithdrawal(t, grm, "0xstaker11b", "0xoperator11", nativeEthStrategy, 1001, "100000000000000000000")

		// Set up staker shares model to process slashing event
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := stakerShares.NewStakerSharesModel(esm, grm, l, cfg)
		require.NoError(t, err)
		err = sharesModel.SetupStateForBlock(1005)
		require.NoError(t, err)

		sp := &SlashingProcessor{
			logger:       l,
			grm:          grm,
			globalConfig: cfg,
		}

		// Beacon chain slash only affects staker11a - process through staker shares model
		change, err := processBeaconChainSlashing(sharesModel, cfg.GetContractsMapForChain().EigenpodManager, 1005, 1, "0xstaker11a", 1e18, 5e17)
		require.NoError(t, err)
		diffs := change.(*stakerShares.AccumulatedStateChanges)
		require.Equal(t, 1, len(diffs.SlashDiffs))
		slashDiff := diffs.SlashDiffs[0]
		beaconSlashEvent := &SlashingEvent{
			SlashedEntity:   slashDiff.SlashedEntity,
			BeaconChain:     slashDiff.BeaconChain,
			Strategy:        slashDiff.Strategy,
			WadSlashed:      slashDiff.WadSlashed.String(),
			TransactionHash: slashDiff.TransactionHash,
			LogIndex:        slashDiff.LogIndex,
		}
		err = sp.createSlashingAdjustments(beaconSlashEvent, 1005, nil)
		require.NoError(t, err)

		// Verify only staker11a has adjustment
		var countA, countB int64
		res := grm.Raw(`SELECT COUNT(*) FROM queued_withdrawal_slashing_adjustments WHERE staker = ?`, "0xstaker11a").Scan(&countA)
		require.NoError(t, res.Error)
		res = grm.Raw(`SELECT COUNT(*) FROM queued_withdrawal_slashing_adjustments WHERE staker = ?`, "0xstaker11b").Scan(&countB)
		require.NoError(t, res.Error)

		assert.Equal(t, int64(1), countA, "Staker11a should have adjustment")
		assert.Equal(t, int64(0), countB, "Staker11b should NOT have adjustment (beacon chain slash is staker-specific)")
	})
}

// ============================================================================
// CSA Test Helper Functions
// ============================================================================

func setupCSATest() (string, *gorm.DB, *zap.Logger, *config.Config, error) {
	cfg := config.NewConfig()
	cfg.Chain = config.Chain_PreprodHoodi
	cfg.Debug = false
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()
	cfg.Rewards.WithdrawalQueueWindow = 14 // 14 days

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func cleanupCSATest(t *testing.T, grm *gorm.DB) {
	queries := []string{
		`truncate table queued_withdrawal_slashing_adjustments cascade`,
		`truncate table queued_slashing_withdrawals cascade`,
		`truncate table slashed_operator_shares cascade`,
		`truncate table blocks cascade`,
	}
	for _, query := range queries {
		res := grm.Exec(query)
		require.NoError(t, res.Error, "Failed to cleanup: "+query)
	}
}

func setupBlocksForCSA(t *testing.T, grm *gorm.DB, blockNumbers []uint64) {
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

func setupBlocksWithTime(t *testing.T, grm *gorm.DB, blocks []struct {
	number uint64
	time   time.Time
}) {
	for _, block := range blocks {
		res := grm.Exec(`
			INSERT INTO blocks (number, hash, block_time)
			VALUES (?, ?, ?)
		`, block.number, fmt.Sprintf("hash_%d", block.number), block.time)
		require.NoError(t, res.Error, fmt.Sprintf("Failed to insert block %d", block.number))
	}
}

func insertQueuedWithdrawal(t *testing.T, grm *gorm.DB, staker, operator, strategy string, blockNumber uint64, shares string) {
	insertQueuedWithdrawalWithLogIndex(t, grm, staker, operator, strategy, blockNumber, 1, shares)
}

func insertQueuedWithdrawalWithLogIndex(t *testing.T, grm *gorm.DB, staker, operator, strategy string, blockNumber uint64, logIndex uint64, shares string) {
	res := grm.Exec(`
		INSERT INTO queued_slashing_withdrawals (
			staker, operator, withdrawer, nonce, start_block, strategy,
			scaled_shares, shares_to_withdraw, withdrawal_root,
			block_number, transaction_hash, log_index
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, staker, operator, staker, "1", blockNumber, strategy,
		shares, shares, "root_"+staker,
		blockNumber, fmt.Sprintf("tx_%d", blockNumber), logIndex)
	require.NoError(t, res.Error, "Failed to insert queued withdrawal")
}

// processBeaconChainSlashing processes a beacon chain slashing event through the staker shares model
// prevBeaconChainScalingFactor and newBeaconChainScalingFactor are the scaling factors before and after the slash
// The wadSlashed is calculated as: (prev - new) / prev * 1e18
func processBeaconChainSlashing(sharesModel *stakerShares.StakerSharesModel, eigenpodManager string, blockNumber, logIndex uint64, staker string, prevBeaconChainScalingFactor, newBeaconChainScalingFactor uint64) (interface{}, error) {
	beaconChainSlashingFactorDecreasedEvent := stakerShares.BeaconChainSlashingFactorDecreasedOutputData{
		Staker:                        staker,
		PrevBeaconChainSlashingFactor: prevBeaconChainScalingFactor,
		NewBeaconChainSlashingFactor:  newBeaconChainScalingFactor,
	}
	beaconChainSlashingFactorDecreasedJson, err := json.Marshal(beaconChainSlashingFactorDecreasedEvent)
	if err != nil {
		return nil, err
	}

	beaconChainSlashingFactorDecreasedLog := storage.TransactionLog{
		TransactionHash:  "some hash",
		TransactionIndex: 100,
		BlockNumber:      blockNumber,
		Address:          eigenpodManager,
		Arguments:        ``,
		EventName:        "BeaconChainSlashingFactorDecreased",
		LogIndex:         logIndex,
		OutputData:       string(beaconChainSlashingFactorDecreasedJson),
		CreatedAt:        time.Time{},
		UpdatedAt:        time.Time{},
		DeletedAt:        time.Time{},
	}

	return sharesModel.HandleStateChange(&beaconChainSlashingFactorDecreasedLog)
}
