package stakerShares

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerDelegations"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/stretchr/testify/assert"
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

func Test_StakerSharesState(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should create a new OperatorSharesState", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, sharesModel)
	})
	t.Run("Should handle an M1 withdrawal and migration to M2 correctly", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		// --------------------------------------------------------------------
		// Deposit
		block := storage.Block{
			Number: 18816124,
			Hash:   "some hash",
		}
		res := grm.Model(storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		transaction := storage.Transaction{
			BlockNumber:      block.Number,
			TransactionHash:  "0x555472583922cc175caf63496f3a83d29f45ad6570eeced2d0f7d50a6716e93b",
			TransactionIndex: big.NewInt(200).Uint64(),
			FromAddress:      "0x858646372cc42e1a627fce94aa7a7033e7cf075a",
		}
		res = grm.Model(storage.Transaction{}).Create(&transaction)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		// Insert deposit
		depositTx := storage.TransactionLog{
			TransactionHash:  "0x555472583922cc175caf63496f3a83d29f45ad6570eeced2d0f7d50a6716e93b",
			TransactionIndex: transaction.TransactionIndex,
			BlockNumber:      transaction.BlockNumber,
			Address:          cfg.GetContractsMapForChain().StrategyManager,
			Arguments:        `[{"Name": "depositor", "Type": "address", "Value": null, "Indexed": false}, {"Name": "token", "Type": "address", "Value": null, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": null, "Indexed": false}, {"Name": "shares", "Type": "uint256", "Value": null, "Indexed": false}]`,
			EventName:        "Deposit",
			LogIndex:         big.NewInt(229).Uint64(),
			OutputData:       `{"token": "0xf951e335afb289353dc249e82926178eac7ded78", "shares": 502179505706314959, "strategy": "0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6", "depositor": "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&depositTx)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		err = sharesModel.SetupStateForBlock(transaction.BlockNumber)
		assert.Nil(t, err)

		change, err := sharesModel.HandleStateChange(&depositTx)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		typedChange := change.(*AccumulatedStateChanges)

		assert.Equal(t, 1, len(typedChange.ShareDeltas))
		assert.Equal(t, "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0", typedChange.ShareDeltas[0].Staker)
		assert.Equal(t, "0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6", typedChange.ShareDeltas[0].Strategy)
		assert.Equal(t, "502179505706314959", typedChange.ShareDeltas[0].Shares)

		shareDeltas, ok := sharesModel.shareDeltaAccumulator[block.Number]
		assert.True(t, ok)
		assert.NotNil(t, shareDeltas)
		assert.Equal(t, "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0", shareDeltas[0].Staker)
		assert.Equal(t, "0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6", shareDeltas[0].Strategy)
		assert.Equal(t, "502179505706314959", shareDeltas[0].Shares)

		err = sharesModel.CommitFinalState(transaction.BlockNumber, false)
		assert.Nil(t, err)

		// --------------------------------------------------------------------
		// M1 Withdrawal
		block = storage.Block{
			Number: 19518613,
			Hash:   "some hash",
		}
		res = grm.Model(storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		transaction = storage.Transaction{
			BlockNumber:      block.Number,
			TransactionHash:  "0xa6315a9a988d3c643ce123ca3a218913ea14cf3e0f51b720488bb2367fc75465",
			TransactionIndex: big.NewInt(128).Uint64(),
			FromAddress:      "0x858646372cc42e1a627fce94aa7a7033e7cf075a",
		}
		res = grm.Model(storage.Transaction{}).Create(&transaction)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		err = sharesModel.SetupStateForBlock(transaction.BlockNumber)
		assert.Nil(t, err)

		shareWithdrawalQueuedTx := &storage.TransactionLog{
			TransactionHash:  transaction.TransactionHash,
			TransactionIndex: transaction.TransactionIndex,
			BlockNumber:      transaction.BlockNumber,
			Address:          "0x858646372cc42e1a627fce94aa7a7033e7cf075a",
			Arguments:        `[{"Name": "depositor", "Type": "address", "Value": null, "Indexed": false}, {"Name": "nonce", "Type": "uint96", "Value": null, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": null, "Indexed": false}, {"Name": "shares", "Type": "uint256", "Value": null, "Indexed": false}]`,
			EventName:        "ShareWithdrawalQueued",
			LogIndex:         302,
			OutputData:       `{"nonce": 0, "shares": 502179505706314959, "strategy": "0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6", "depositor": "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&shareWithdrawalQueuedTx)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		withdrawalQueuedTx := &storage.TransactionLog{
			TransactionHash:  transaction.TransactionHash,
			TransactionIndex: transaction.TransactionIndex,
			BlockNumber:      transaction.BlockNumber,
			Address:          "0x858646372cc42e1a627fce94aa7a7033e7cf075a",
			Arguments:        `[{"Name": "depositor", "Type": "address", "Value": null, "Indexed": false}, {"Name": "nonce", "Type": "uint96", "Value": null, "Indexed": false}, {"Name": "withdrawer", "Type": "address", "Value": null, "Indexed": false}, {"Name": "delegatedAddress", "Type": "address", "Value": null, "Indexed": false}, {"Name": "withdrawalRoot", "Type": "bytes32", "Value": null, "Indexed": false}]`,
			EventName:        "WithdrawalQueued",
			LogIndex:         303,
			OutputData:       `{"nonce": 0, "depositor": "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0", "withdrawer": "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0", "withdrawalRoot": [181, 96, 205, 58, 97, 121, 217, 167, 18, 132, 193, 76, 115, 179, 69, 201, 63, 185, 242, 68, 128, 94, 225, 114, 13, 173, 1, 156, 214, 81, 24, 83], "delegatedAddress": "0x0000000000000000000000000000000000000000"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&withdrawalQueuedTx)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		change, err = sharesModel.HandleStateChange(shareWithdrawalQueuedTx)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		change, err = sharesModel.HandleStateChange(withdrawalQueuedTx)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		err = sharesModel.CommitFinalState(transaction.BlockNumber, false)
		assert.Nil(t, err)

		// --------------------------------------------------------------------
		// M2 migration
		block = storage.Block{
			Number: 19612227,
			Hash:   "some hash",
		}
		res = grm.Model(storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		transaction = storage.Transaction{
			BlockNumber:      block.Number,
			TransactionHash:  "0xf231201ad19e9d35a72d0269a1a9a01236986525449da3e2ea42124fb4410aac",
			TransactionIndex: big.NewInt(128).Uint64(),
			FromAddress:      "0x39053d51b77dc0d36036fc1fcc8cb819df8ef37a",
		}
		res = grm.Model(storage.Transaction{}).Create(&transaction)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		err = sharesModel.SetupStateForBlock(transaction.BlockNumber)
		assert.Nil(t, err)

		withdrawalQueued := &storage.TransactionLog{
			TransactionHash:  transaction.TransactionHash,
			TransactionIndex: transaction.TransactionIndex,
			BlockNumber:      transaction.BlockNumber,
			Address:          "0x39053d51b77dc0d36036fc1fcc8cb819df8ef37a",
			Arguments:        `[{"Name": "withdrawalRoot", "Type": "bytes32", "Value": null, "Indexed": false}, {"Name": "withdrawal", "Type": "(address,address,address,uint256,uint32,address[],uint256[])", "Value": null, "Indexed": false}]`,
			EventName:        "WithdrawalQueued",
			LogIndex:         207,
			OutputData:       `{"withdrawal": {"nonce": 0, "shares": [502179505706314959], "staker": "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0", "startBlock": 19518613, "strategies": ["0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6"], "withdrawer": "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0", "delegatedTo": "0x0000000000000000000000000000000000000000"}, "withdrawalRoot": [169, 79, 1, 179, 199, 73, 184, 145, 60, 107, 232, 188, 151, 104, 19, 21, 140, 92, 208, 223, 223, 213, 246, 143, 171, 232, 217, 181, 177, 46, 115, 78]}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&withdrawalQueued)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		withdrawalMigrated := &storage.TransactionLog{
			TransactionHash:  transaction.TransactionHash,
			TransactionIndex: transaction.TransactionIndex,
			BlockNumber:      transaction.BlockNumber,
			Address:          "0x39053d51b77dc0d36036fc1fcc8cb819df8ef37a",
			Arguments:        `[{"Name": "oldWithdrawalRoot", "Type": "bytes32", "Value": null, "Indexed": false}, {"Name": "newWithdrawalRoot", "Type": "bytes32", "Value": null, "Indexed": false}]`,
			EventName:        "WithdrawalMigrated",
			LogIndex:         208,
			OutputData:       `{"newWithdrawalRoot": [169, 79, 1, 179, 199, 73, 184, 145, 60, 107, 232, 188, 151, 104, 19, 21, 140, 92, 208, 223, 223, 213, 246, 143, 171, 232, 217, 181, 177, 46, 115, 78], "oldWithdrawalRoot": [181, 96, 205, 58, 97, 121, 217, 167, 18, 132, 193, 76, 115, 179, 69, 201, 63, 185, 242, 68, 128, 94, 225, 114, 13, 173, 1, 156, 214, 81, 24, 83]}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&withdrawalMigrated)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		change, err = sharesModel.HandleStateChange(withdrawalQueued)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		change, err = sharesModel.HandleStateChange(withdrawalMigrated)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		err = sharesModel.CommitFinalState(transaction.BlockNumber, false)
		assert.Nil(t, err)

		// --------------------------------------------------------------------
		// Deposit
		block = storage.Block{
			Number: 20104478,
			Hash:   "some hash",
		}
		res = grm.Model(storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		transaction = storage.Transaction{
			BlockNumber:      block.Number,
			TransactionHash:  "0x75ab8bde9be4282d7eeff081b6510f1d076d2b739c0524d3080182828ca412c4",
			TransactionIndex: big.NewInt(128).Uint64(),
			ToAddress:        "0x858646372cc42e1a627fce94aa7a7033e7cf075a",
		}
		res = grm.Model(storage.Transaction{}).Create(&transaction)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		err = sharesModel.SetupStateForBlock(transaction.BlockNumber)
		assert.Nil(t, err)

		deposit2 := &storage.TransactionLog{
			TransactionHash:  transaction.TransactionHash,
			TransactionIndex: transaction.TransactionIndex,
			BlockNumber:      transaction.BlockNumber,
			Address:          "0x858646372cc42e1a627fce94aa7a7033e7cf075a",
			Arguments:        `[{"Name": "staker", "Type": "address", "Value": null, "Indexed": false}, {"Name": "token", "Type": "address", "Value": null, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": null, "Indexed": false}, {"Name": "shares", "Type": "uint256", "Value": null, "Indexed": false}]`,
			EventName:        "Deposit",
			LogIndex:         540,
			OutputData:       `{"token": "0xec53bf9167f50cdeb3ae105f56099aaab9061f83", "shares": 126014635232337198545, "staker": "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0", "strategy": "0xacb55c530acdb2849e6d4f36992cd8c9d50ed8f7"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&deposit2)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		change, err = sharesModel.HandleStateChange(deposit2)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		err = sharesModel.CommitFinalState(transaction.BlockNumber, false)
		assert.Nil(t, err)

		query := `select * from staker_share_deltas order by block_number asc`
		results := []StakerShareDeltas{}
		res = sharesModel.DB.Raw(query).Scan(&results)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		assert.Equal(t, 3, len(results))

		query = `
		with combined_values as (
			select
				staker,
				strategy,
				log_index,
				block_number,
				SUM(shares) OVER (PARTITION BY staker, strategy order by block_number, log_index) as shares
			from staker_share_deltas
		)
		select * from combined_values order by block_number asc, log_index asc
		`
		type resultsRow struct {
			Staker      string
			Strategy    string
			LogIndex    uint64
			BlockNumber uint64
			Shares      string
		}
		var shareResults []resultsRow
		res = grm.Raw(query).Scan(&shareResults)
		assert.Nil(t, res.Error)

		expectedResults := []resultsRow{
			resultsRow{
				Staker:      "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0",
				Strategy:    "0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6",
				Shares:      "502179505706314959",
				LogIndex:    229,
				BlockNumber: 18816124,
			},
			resultsRow{
				Staker:      "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0",
				Strategy:    "0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6",
				Shares:      "0",
				LogIndex:    302,
				BlockNumber: 19518613,
			},
			resultsRow{
				Staker:      "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0",
				Strategy:    "0xacb55c530acdb2849e6d4f36992cd8c9d50ed8f7",
				Shares:      "126014635232337198545",
				LogIndex:    540,
				BlockNumber: 20104478,
			},
		}

		for i, result := range shareResults {
			assert.Equal(t, expectedResults[i].Staker, result.Staker)
			assert.Equal(t, expectedResults[i].Strategy, result.Strategy)
			assert.Equal(t, expectedResults[i].Shares, result.Shares)
			assert.Equal(t, expectedResults[i].LogIndex, result.LogIndex)
			assert.Equal(t, expectedResults[i].BlockNumber, result.BlockNumber)
		}

		// --------------------------------------------------------------------
		// EigenPod deposit

		block = storage.Block{
			Number: 20468489,
			Hash:   "some hash",
		}
		res = grm.Model(storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		transaction = storage.Transaction{
			BlockNumber:      block.Number,
			TransactionHash:  "0xcaa01689e4f1a3ea35f0d632e43bb0991e674148f9b5e8ed8e03d8ba88cf7eba",
			TransactionIndex: big.NewInt(128).Uint64(),
			ToAddress:        "0x91e677b07f7af907ec9a428aafa9fc14a0d3a338",
		}
		res = grm.Model(storage.Transaction{}).Create(&transaction)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		err = sharesModel.SetupStateForBlock(transaction.BlockNumber)
		assert.Nil(t, err)

		log := storage.TransactionLog{
			TransactionHash:  transaction.TransactionHash,
			TransactionIndex: transaction.TransactionIndex,
			BlockNumber:      transaction.BlockNumber,
			Address:          cfg.GetContractsMapForChain().EigenpodManager,
			Arguments:        `[{"Name": "podOwner", "Type": "address", "Value": "0x049ea11d337f185b1aa910d98e8fbd991f0fba7b", "Indexed": true}, {"Name": "sharesDelta", "Type": "int256", "Value": null, "Indexed": false}]`,
			EventName:        "PodSharesUpdated",
			LogIndex:         big.NewInt(188).Uint64(),
			OutputData:       `{"sharesDelta": 32000000000000000000}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err = sharesModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		change, err = sharesModel.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		typedChange = change.(*AccumulatedStateChanges)
		assert.Equal(t, 1, len(typedChange.ShareDeltas))

		assert.Equal(t, "32000000000000000000", typedChange.ShareDeltas[0].Shares)
		assert.Equal(t, strings.ToLower("0x049ea11d337f185b1aa910d98e8fbd991f0fba7b"), typedChange.ShareDeltas[0].Staker)
		assert.Equal(t, NativeEthStrategy, typedChange.ShareDeltas[0].Strategy)

		err = sharesModel.CommitFinalState(transaction.BlockNumber, false)
		assert.Nil(t, err)

		var count int
		res = grm.Raw(`select count(*) from staker_share_deltas`).Scan(&count)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		assert.Equal(t, 4, count)
	})
	t.Run("Should capture Slashing withdrawals", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(300).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			Arguments:        `[{"Name": "withdrawalRoot", "Type": "bytes32", "Value": null, "Indexed": false}, {"Name": "withdrawal", "Type": "(address,address,address,uint256,uint32,address[],uint256[])", "Value": null, "Indexed": false}, {"Name": "sharesToWithdraw", "Type": "uint256[]", "Value": null, "Indexed": false}]`,
			EventName:        "SlashingWithdrawalQueued",
			LogIndex:         big.NewInt(600).Uint64(),
			OutputData:       `{"withdrawal": {"nonce": 6, "staker": "0x8e4662c95c2206fa22b408426f2b457672674963", "startBlock": 2959439, "strategies": ["0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0"], "withdrawer": "0x8e4662c95c2206fa22b408426f2b457672674963", "delegatedTo": "0xfbf1ba7e299899ba98e1bf85d41a3eeba2723c4b", "scaledShares": [5000000000000000]}, "withdrawalRoot": [76, 194, 71, 10, 54, 85, 8, 13, 85, 221, 58, 23, 242, 148, 246, 156, 213, 7, 64, 204, 190, 139, 211, 169, 7, 248, 24, 6, 218, 15, 96, 172], "sharesToWithdraw": [5000000000000000]}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := sharesModel.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		diffs := change.(*AccumulatedStateChanges)
		assert.Equal(t, 1, len(diffs.ShareDeltas))

		shareDiff := diffs.ShareDeltas[0]
		assert.Equal(t, "-5000000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x8e4662c95c2206fa22b408426f2b457672674963"), shareDiff.Staker)
		assert.Equal(t, NativeEthStrategy, shareDiff.Strategy)

		preparedState, err := sharesModel.prepareState(blockNumber)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(preparedState))
	})
	t.Run("Should capture Slashing withdrawals for multiple strategies", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(300).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			Arguments:        `[{"Name": "withdrawalRoot", "Type": "bytes32", "Value": ""}, {"Name": "withdrawal", "Type": "(address,address,address,uint256,uint32,address[],uint256[])", "Value": ""}]`,
			EventName:        "SlashingWithdrawalQueued",
			LogIndex:         big.NewInt(600).Uint64(),
			OutputData:       `{"withdrawal": {"nonce": 0, "scaledShares": [1000000000000000000, 2000000000000000000], "staker": "0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c", "startBlock": 1215690, "strategies": ["0xd523267698c81a372191136e477fdebfa33d9fb4", "0xe523267698c81a372191136e477fdebfa33d9fb5"], "withdrawer": "0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c", "delegatedTo": "0x2177dee1f66d6dbfbf517d9c4f316024c6a21aeb"}, "withdrawalRoot": [24, 23, 49, 137, 14, 63, 119, 12, 234, 225, 63, 35, 109, 249, 112, 24, 241, 118, 212, 52, 22, 107, 202, 56, 105, 37, 68, 47, 169, 23, 142, 135], "sharesToWithdraw": [50000000000000, 100000000000000]}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := sharesModel.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		diffs := change.(*AccumulatedStateChanges)
		assert.Equal(t, 2, len(diffs.ShareDeltas))

		shareDiff := diffs.ShareDeltas[0]
		assert.Equal(t, "-50000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xd523267698c81a372191136e477fdebfa33d9fb4", shareDiff.Strategy)

		shareDiff = diffs.ShareDeltas[1]
		assert.Equal(t, "-100000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xe523267698c81a372191136e477fdebfa33d9fb5", shareDiff.Strategy)
	})
	t.Run("Should capture many deposits and slash in a different block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
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

		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// ----------------------
		// New block
		// ----------------------
		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `
				select * from staker_share_deltas
				where block_number = ?
				order by log_index, staker asc
			`
		results := []*StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 2, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "-100000000000000000", results[0].Shares)

		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", results[1].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[1].Strategy)
		assert.Equal(t, "-200000000000000000", results[1].Shares)

		teardown(grm)
	})
	t.Run("Should slash deposits and delegations in previous block with greater logIndex", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// ----------------------
		// Handle events
		// ----------------------
		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 10000, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 1000, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)
		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// ------------------------
		// New block
		// ------------------------
		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `
				select * from staker_share_deltas
				where block_number = ?
				order by log_index, staker asc
			`
		results := []*StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "-100000000000000000", results[0].Shares)

		teardown(grm)
	})
	t.Run("Should not slash delegated staker in a different strategy for a deposit in same block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// ----------------------
		// Handle events
		// ----------------------
		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x1234567890abcdef1234567890abcdef12345678", big.NewInt(1e18))
		assert.Nil(t, err)

		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateChanges)
		assert.Equal(t, 1, len(diffs.SlashDiffs))

		slashDiff := diffs.SlashDiffs[0]
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.SlashedEntity)
		assert.False(t, slashDiff.BeaconChain)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadSlashed.String())

		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)
		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `
				select * from staker_share_deltas
				where block_number = ?
				order by log_index, staker asc
			`
		results := []*StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", results[0].Strategy)
		assert.Equal(t, "1000000000000000000", results[0].Shares)

		teardown(grm)
	})
	t.Run("Should not slash delegated staker in a different strategy deposited in previous block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)
		blockNumber := uint64(200)

		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// ----------------------
		// Handle events
		// ----------------------
		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x1234567890abcdef1234567890abcdef12345678", big.NewInt(1e18))
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)
		query := `select * from staker_share_deltas where block_number = ?`
		results := []StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, len(results))
		// ------------------------
		// New block
		// ------------------------
		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query = `select * from staker_share_deltas`
		results = []StakerShareDeltas{}
		res = sharesModel.DB.Raw(query).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", results[0].Strategy)
		assert.Equal(t, "1000000000000000000", results[0].Shares)
		assert.Equal(t, blockNumber-1, results[0].BlockNumber)

		teardown(grm)
	})
	t.Run("Should not slash deposit after slashing in same block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
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

		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)
		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// ------------------------
		// New block
		// ------------------------
		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 600, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `
				select * from staker_share_deltas
				where block_number = ?
				order by log_index, staker asc
			`
		results := []*StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 2, len(results))

		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "-100000000000000000", results[0].Shares)

		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[1].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[1].Strategy)
		assert.Equal(t, "1000000000000000000", results[1].Shares)

		teardown(grm)
	})
	t.Run("Should process slashing for several strategies correctly", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// ----------------------
		// Handle events
		// ----------------------
		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 301, "0x4444444444444444444444444444444444444444", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)

		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 401, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x1234567890abcdef1234567890abcdef12345678", big.NewInt(2e18))
		assert.Nil(t, err)

		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 401, "0x4444444444444444444444444444444444444444", "0x1234567890abcdef1234567890abcdef12345678", big.NewInt(4e18))
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)
		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// ------------------------
		// New block
		// ------------------------
		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", "0x1234567890abcdef1234567890abcdef12345678"}, []*big.Int{big.NewInt(1e17), big.NewInt(9e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateChanges)
		assert.Equal(t, 2, len(diffs.SlashDiffs))

		slashDiff := diffs.SlashDiffs[0]
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.SlashedEntity)
		assert.False(t, slashDiff.BeaconChain)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadSlashed.String())

		slashDiff = diffs.SlashDiffs[1]
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.SlashedEntity)
		assert.False(t, slashDiff.BeaconChain)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", slashDiff.Strategy)
		assert.Equal(t, "900000000000000000", slashDiff.WadSlashed.String())

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `
				select * from staker_share_deltas
				where block_number = ?
				order by log_index, staker, strategy asc
			`
		results := []*StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 3, len(results))

		// assert.Equal(t, "0x4444444444444444444444444444444444444444", results[0].Staker)
		// assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", results[0].Strategy)
		// assert.Equal(t, "-3600000000000000000", results[0].Shares)
		//
		// assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[1].Staker)
		// assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", results[1].Strategy)
		// assert.Equal(t, "-1800000000000000000", results[1].Shares)
		//
		// assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[2].Staker)
		// assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[2].Strategy)
		// assert.Equal(t, "-100000000000000000", results[2].Shares)

		teardown(grm)
	})
	t.Run("Should handle a full slashing", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
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

		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)
		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// ------------------------
		// New block
		// ------------------------
		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e18)})
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `
				select * from staker_share_deltas
				where block_number = ?
			`
		results := []*StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "-1000000000000000000", results[0].Shares)

		teardown(grm)
	})
	t.Run("Should not slash when staker has 0 shares", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// ----------------------
		// Handle events
		// ----------------------
		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(0))
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)
		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// ------------------------
		// New block
		// ------------------------
		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateChanges)
		assert.Equal(t, 1, len(diffs.SlashDiffs))

		slashDiff := diffs.SlashDiffs[0]
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.SlashedEntity)
		assert.False(t, slashDiff.BeaconChain)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadSlashed.String())

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `
				select * from staker_share_deltas
				where block_number = ?
			`
		results := []*StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 0, len(results))

		teardown(grm)
	})
	t.Run("Should handle beacon chain slashing of deposit in same block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// Process a deposit of beacon chain ETH
		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400,
			"0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d",
			NativeEthStrategy, // Beacon chain strategy address
			big.NewInt(1e18))
		assert.Nil(t, err)

		// Process beacon chain slashing in same block
		change, err := processBeaconChainSlashing(sharesModel, cfg.GetContractsMapForChain().EigenpodManager,
			blockNumber, 500,
			"0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d",
			1e18, 9e17) // 10% slash
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateChanges)
		assert.Equal(t, 1, len(diffs.SlashDiffs))

		slashDiff := diffs.SlashDiffs[0]
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", slashDiff.SlashedEntity)
		assert.True(t, slashDiff.BeaconChain)
		assert.Equal(t, NativeEthStrategy, slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadSlashed.String())

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// Query the database to verify the share deltas
		query := `
			select * from staker_share_deltas
			where block_number = ?
			order by log_index asc
		`
		results := []*StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 2, len(results))

		// First record should be the deposit
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, NativeEthStrategy, results[0].Strategy)
		assert.Equal(t, "1000000000000000000", results[0].Shares)

		// Second record should be the slash
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[1].Staker)
		assert.Equal(t, NativeEthStrategy, results[1].Strategy)
		assert.Equal(t, "-100000000000000000", results[1].Shares)

		teardown(grm)
	})
	t.Run("Should handle beacon chain slashing of deposit in previous block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// Process a deposit of beacon chain ETH
		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400,
			"0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d",
			NativeEthStrategy, // Beacon chain strategy address
			big.NewInt(1e18))
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// Process beacon chain slashing
		_, err = processBeaconChainSlashing(sharesModel, cfg.GetContractsMapForChain().EigenpodManager,
			blockNumber, 500,
			"0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d",
			1e18, 9e17)
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// Query the database to verify the share deltas
		query := `
			select * from staker_share_deltas
			where block_number = ?
			order by log_index asc
		`
		results := []*StakerShareDeltas{}
		res := sharesModel.
			DB.Raw(query, blockNumber).
			Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))

		// First record should be the slash
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, NativeEthStrategy, results[0].Strategy)
		assert.Equal(t, "-100000000000000000", results[0].Shares)

		teardown(grm)
	})
	t.Run("Should handle beacon chain slashing and eigenlayer slashing in same block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		stakerShares, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = stakerShares.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// ----------------------
		// Handle events
		// ----------------------
		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		_, err = processDeposit(stakerShares, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 401, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", NativeEthStrategy, big.NewInt(1e18))
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)
		err = stakerShares.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// ------------------------
		// New block
		// ------------------------
		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = stakerShares.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processSlashing(stakerShares, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{NativeEthStrategy}, []*big.Int{big.NewInt(5e17)})
		assert.Nil(t, err)

		_, err = processBeaconChainSlashing(stakerShares, cfg.GetContractsMapForChain().EigenpodManager, blockNumber, 600, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", 1e18, 5e17)
		assert.Nil(t, err)

		err = stakerShares.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `
				select * from staker_share_deltas
				where block_number = ?
				order by log_index, staker asc
			`
		results := []*StakerShareDeltas{}
		res := stakerShares.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 2, len(results))

		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, NativeEthStrategy, results[0].Strategy)
		assert.Equal(t, "-500000000000000000", results[0].Shares)

		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[1].Staker)
		assert.Equal(t, NativeEthStrategy, results[1].Strategy)
		assert.Equal(t, "-250000000000000000", results[1].Shares)

		teardown(grm)
	})
	t.Run("Should handle beacon chain slashing in a block after eigenlayer slashing", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// ----------------------
		// Handle events
		// ----------------------
		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 401, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", NativeEthStrategy, big.NewInt(1e18))
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)
		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// ------------------------
		// New block
		// ------------------------
		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processSlashing(sharesModel, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{NativeEthStrategy}, []*big.Int{big.NewInt(5e17)})
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processBeaconChainSlashing(sharesModel, cfg.GetContractsMapForChain().EigenpodManager, blockNumber, 600, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", 1e18, 5e17)
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		query := `
				select * from staker_share_deltas
				where block_number = ?
				order by log_index, staker asc
			`
		results := []*StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))

		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, NativeEthStrategy, results[0].Strategy)
		assert.Equal(t, "-250000000000000000", results[0].Shares)

		teardown(grm)
	})
	t.Run("Should handle full beacon slashing", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// Process a deposit of beacon chain ETH
		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400,
			"0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d",
			NativeEthStrategy, // Beacon chain strategy address
			big.NewInt(1e18))
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// Process beacon chain slashing
		_, err = processBeaconChainSlashing(sharesModel, cfg.GetContractsMapForChain().EigenpodManager,
			blockNumber, 500, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", 1e18, 0)
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// Query the database to verify the share deltas
		query := `
			select * from staker_share_deltas
			where block_number = ?
			order by log_index asc
		`
		results := []*StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))

		// First record should be the slash
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, NativeEthStrategy, results[0].Strategy)
		assert.Equal(t, "-1000000000000000000", results[0].Shares)

		teardown(grm)
	})
	t.Run("Should not beacon chain slash when staker has 0 shares", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// Process a deposit of beacon chain ETH
		_, err = processDeposit(sharesModel, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400,
			"0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", // Staker
			NativeEthStrategy, // Beacon chain strategy address
			big.NewInt(0))
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		blockNumber = blockNumber + 1
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		// Process beacon chain slashing
		_, err = processBeaconChainSlashing(sharesModel, cfg.GetContractsMapForChain().EigenpodManager,
			blockNumber, 500, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", 1e18, 9e17)
		assert.Nil(t, err)

		err = sharesModel.CommitFinalState(blockNumber, false)
		assert.Nil(t, err)

		// Query the database to verify the share deltas
		query := `
			select * from staker_share_deltas
			where block_number = ?
			order by log_index asc
		`

		results := []*StakerShareDeltas{}
		res := sharesModel.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 0, len(results))

		teardown(grm)
	})
	t.Run("Should handle failed unmarshalling gracefully", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		sharesModel, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		blockNumber := uint64(200)
		err = createBlock(grm, blockNumber)
		assert.Nil(t, err)

		err = sharesModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		beaconChainSlashingFactorDecreasedLog := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: 100,
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().EigenpodManager,
			Arguments:        ``,
			EventName:        "BeaconChainSlashingFactorDecreased",
			LogIndex:         400,
			OutputData:       "i'm a bad log output data json",
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		_, err = sharesModel.HandleStateChange(&beaconChainSlashingFactorDecreasedLog)
		assert.Error(t, err)
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

func processDeposit(stakerSharesModel *StakerSharesModel, strategyManager string, blockNumber, logIndex uint64, staker, strategy string, shares *big.Int) (interface{}, error) {
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

func processSlashing(stakerSharesModel *StakerSharesModel, allocationManager string, blockNumber, logIndex uint64, operator string, strategies []string, wadSlashed []*big.Int) (interface{}, error) {
	wadSlashedJson := make([]json.Number, len(wadSlashed))
	for i, wad := range wadSlashed {
		wadSlashedJson[i] = json.Number(wad.String())
	}

	operatorSlashedEvent := OperatorSlashedOutputData{
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

func processBeaconChainSlashing(stakerSharesModel *StakerSharesModel, eigenpodManager string, blockNumber, logIndex uint64, staker string, prevBeaconChainScalingFactor, newBeaconChainScalingFactor uint64) (interface{}, error) {
	beaconChainSlashingFactorDecreasedEvent := BeaconChainSlashingFactorDecreasedOutputData{
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

	return stakerSharesModel.HandleStateChange(&beaconChainSlashingFactorDecreasedLog)
}
