package stakerShares

import (
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
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

func logChanges(changes *AccumulatedStateChanges) {
	// for _, change := range changes.Changes {
	// 	// fmt.Printf("Change: %+v\n", change)
	// }
}

func Test_StakerSharesState(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Should create a new OperatorSharesState", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)
		assert.NotNil(t, model)
	})
	t.Run("Should handle an M1 withdrawal and migration to M2 correctly", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		model, err := NewStakerSharesModel(esm, grm, l, cfg)
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

		err = model.SetupStateForBlock(transaction.BlockNumber)
		assert.Nil(t, err)

		change, err := model.HandleStateChange(&depositTx)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		typedChange := change.(*AccumulatedStateChanges)
		logChanges(typedChange)

		assert.Equal(t, 1, len(typedChange.Changes))
		assert.Equal(t, "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0", typedChange.Changes[0].Staker)
		assert.Equal(t, "0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6", typedChange.Changes[0].Strategy)
		assert.Equal(t, "502179505706314959", typedChange.Changes[0].Shares)

		accumulatedState, ok := model.stateAccumulator[block.Number]
		assert.True(t, ok)
		assert.NotNil(t, accumulatedState)
		assert.Equal(t, "0x00105f70bf0a2dec987dbfc87a869c3090abf6a0", accumulatedState[0].Staker)
		assert.Equal(t, "0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6", accumulatedState[0].Strategy)
		assert.Equal(t, "502179505706314959", accumulatedState[0].Shares)

		err = model.CommitFinalState(transaction.BlockNumber)
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
		err = model.SetupStateForBlock(transaction.BlockNumber)
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

		change, err = model.HandleStateChange(shareWithdrawalQueuedTx)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		typedChange = change.(*AccumulatedStateChanges)
		logChanges(typedChange)

		change, err = model.HandleStateChange(withdrawalQueuedTx)
		assert.Nil(t, err)
		assert.NotNil(t, change)
		typedChange = change.(*AccumulatedStateChanges)
		logChanges(typedChange)

		err = model.CommitFinalState(transaction.BlockNumber)
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
		err = model.SetupStateForBlock(transaction.BlockNumber)
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

		change, err = model.HandleStateChange(withdrawalQueued)
		assert.Nil(t, err)
		assert.NotNil(t, change)
		typedChange = change.(*AccumulatedStateChanges)
		logChanges(typedChange)

		change, err = model.HandleStateChange(withdrawalMigrated)
		assert.Nil(t, err)
		assert.NotNil(t, change)
		typedChange = change.(*AccumulatedStateChanges)
		logChanges(typedChange)

		err = model.CommitFinalState(transaction.BlockNumber)
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
		err = model.SetupStateForBlock(transaction.BlockNumber)
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

		change, err = model.HandleStateChange(deposit2)
		assert.Nil(t, err)
		assert.NotNil(t, change)
		typedChange = change.(*AccumulatedStateChanges)
		logChanges(typedChange)

		err = model.CommitFinalState(transaction.BlockNumber)
		assert.Nil(t, err)

		query := `select * from staker_share_deltas order by block_number asc`
		results := []StakerShareDeltas{}
		res = model.DB.Raw(query).Scan(&results)
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
		err = model.SetupStateForBlock(transaction.BlockNumber)
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

		err = model.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		change, err = model.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		typedChange = change.(*AccumulatedStateChanges)
		assert.Equal(t, 1, len(typedChange.Changes))

		assert.Equal(t, "32000000000000000000", typedChange.Changes[0].Shares)
		assert.Equal(t, strings.ToLower("0x049ea11d337f185b1aa910d98e8fbd991f0fba7b"), typedChange.Changes[0].Staker)
		assert.Equal(t, "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", typedChange.Changes[0].Strategy)

		err = model.CommitFinalState(transaction.BlockNumber)
		assert.Nil(t, err)

		var count int
		res = grm.Raw(`select count(*) from staker_share_deltas`).Scan(&count)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		assert.Equal(t, 4, count)
	})

	t.Run("Should capture Slashing withdrawals", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(300).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			Arguments:        `[{"Name": "withdrawalRoot", "Type": "bytes32", "Value": ""}, {"Name": "withdrawal", "Type": "(address,address,address,uint256,uint32,address[],uint256[])", "Value": ""}]`,
			EventName:        "SlashingWithdrawalQueued",
			LogIndex:         big.NewInt(600).Uint64(),
			OutputData:       `{"withdrawal": {"nonce": 0, "scaledShares": [1000000000000000000], "staker": "0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c", "startBlock": 1215690, "strategies": ["0xd523267698c81a372191136e477fdebfa33d9fb4"], "withdrawer": "0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c", "delegatedTo": "0x2177dee1f66d6dbfbf517d9c4f316024c6a21aeb"}, "withdrawalRoot": [24, 23, 49, 137, 14, 63, 119, 12, 234, 225, 63, 35, 109, 249, 112, 24, 241, 118, 212, 52, 22, 107, 202, 56, 105, 37, 68, 47, 169, 23, 142, 135], "sharesToWithdraw": [50000000000000]}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := model.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		diffs := change.(*AccumulatedStateChanges)
		assert.Equal(t, 1, len(diffs.Changes))

		shareDiff := diffs.Changes[0]
		assert.Equal(t, "-50000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xd523267698c81a372191136e477fdebfa33d9fb4", shareDiff.Strategy)
	})

	t.Run("Should capture Slashing withdrawals for multiple strategies", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
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

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := model.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		diffs := change.(*AccumulatedStateChanges)
		assert.Equal(t, 2, len(diffs.Changes))

		shareDiff := diffs.Changes[0]
		assert.Equal(t, "-50000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xd523267698c81a372191136e477fdebfa33d9fb4", shareDiff.Strategy)

		shareDiff = diffs.Changes[1]
		assert.Equal(t, "-100000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xe523267698c81a372191136e477fdebfa33d9fb5", shareDiff.Strategy)
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
