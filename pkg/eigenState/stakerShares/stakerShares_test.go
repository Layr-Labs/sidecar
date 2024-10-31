package stakerShares

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/go-sidecar/pkg/postgres"
	"github.com/Layr-Labs/go-sidecar/pkg/storage"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/stakerDelegations"
	"github.com/Layr-Labs/go-sidecar/pkg/eigenState/stateManager"
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
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: true})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func teardown(model *StakerSharesModel) {
	queries := []string{
		`truncate table staker_shares cascade`,
		`truncate table blocks cascade`,
		`truncate table transactions cascade`,
		`truncate table transaction_logs cascade`,
		`truncate table delegated_stakers cascade`,
		`truncate table staker_delegation_Diffs cascade`,
	}
	for _, query := range queries {
		model.DB.Exec(query)
	}
}

func createBlock(model *StakerSharesModel, blockNumber uint64) error {
	block := &storage.Block{
		Number:    blockNumber,
		Hash:      "some hash",
		BlockTime: time.Now().Add(time.Hour * time.Duration(blockNumber)),
	}
	res := model.DB.Model(&storage.Block{}).Create(block)
	if res.Error != nil {
		return res.Error
	}
	return nil
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
	t.Run("Should capture a staker share Deposit", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().StrategyManager,
			Arguments:        `[{"Name": "staker", "Type": "address", "Value": ""}, {"Name": "token", "Type": "address", "Value": ""}, {"Name": "strategy", "Type": "address", "Value": ""}, {"Name": "shares", "Type": "uint256", "Value": ""}]`,
			EventName:        "Deposit",
			LogIndex:         big.NewInt(400).Uint64(),
			OutputData:       `{"token": "0x3f1c547b21f65e10480de3ad8e19faac46c95034", "shares": 159925690037480381, "staker": "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "strategy": "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}`,
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

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		shareDiff := diffs.StateDiffs[0].Event.(*StakerShareDeltas)

		assert.Equal(t, "159925690037480381", shareDiff.Shares)
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", shareDiff.Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", shareDiff.Strategy)

		t.Cleanup(func() {
			teardown(model)
		})
	})
	t.Run("Should capture a staker share M1 Withdrawal", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(200).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().StrategyManager,
			Arguments:        `[{"Name": "depositor", "Type": "address", "Value": null, "Indexed": false}, {"Name": "nonce", "Type": "uint96", "Value": null, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": null, "Indexed": false}, {"Name": "shares", "Type": "uint256", "Value": null, "Indexed": false}]`,
			EventName:        "ShareWithdrawalQueued",
			LogIndex:         big.NewInt(500).Uint64(),
			OutputData:       `{"nonce": 0, "shares": 246393621132195985, "strategy": "0x298afb19a105d59e74658c4c334ff360bade6dd2", "depositor": "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78"}`,
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

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		shareDiff := diffs.StateDiffs[0].Event.(*StakerShareDeltas)

		assert.Equal(t, "-246393621132195985", shareDiff.Shares)
		assert.Equal(t, "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", shareDiff.Staker)
		assert.Equal(t, "0x298afb19a105d59e74658c4c334ff360bade6dd2", shareDiff.Strategy)

		t.Cleanup(func() {
			teardown(model)
		})
	})
	t.Run("Should capture staker EigenPod shares", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(300).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().EigenpodManager,
			Arguments:        `[{"Name": "podOwner", "Type": "address", "Value": "0x0808D4689B347D499a96f139A5fC5B5101258406"}, {"Name": "sharesDelta", "Type": "int256", "Value": ""}]`,
			EventName:        "PodSharesUpdated",
			LogIndex:         big.NewInt(600).Uint64(),
			OutputData:       `{"sharesDelta": 32000000000000000000}`,
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

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		shareDiff := diffs.StateDiffs[0].Event.(*StakerShareDeltas)

		assert.Equal(t, "32000000000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x0808D4689B347D499a96f139A5fC5B5101258406"), shareDiff.Staker)
		assert.Equal(t, "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", shareDiff.Strategy)

		t.Cleanup(func() {
			teardown(model)
		})
	})
	t.Run("Should capture M2 withdrawals", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(300).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			Arguments:        `[{"Name": "withdrawalRoot", "Type": "bytes32", "Value": ""}, {"Name": "withdrawal", "Type": "(address,address,address,uint256,uint32,address[],uint256[])", "Value": ""}]`,
			EventName:        "WithdrawalQueued",
			LogIndex:         big.NewInt(600).Uint64(),
			OutputData:       `{"withdrawal": {"nonce": 0, "shares": [1000000000000000000], "staker": "0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c", "startBlock": 1215690, "strategies": ["0xd523267698c81a372191136e477fdebfa33d9fb4"], "withdrawer": "0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c", "delegatedTo": "0x2177dee1f66d6dbfbf517d9c4f316024c6a21aeb"}, "withdrawalRoot": [24, 23, 49, 137, 14, 63, 119, 12, 234, 225, 63, 35, 109, 249, 112, 24, 241, 118, 212, 52, 22, 107, 202, 56, 105, 37, 68, 47, 169, 23, 142, 135]}`,
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

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		shareDiff := diffs.StateDiffs[0].Event.(*StakerShareDeltas)

		assert.Equal(t, "-1000000000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xd523267698c81a372191136e477fdebfa33d9fb4", shareDiff.Strategy)

		t.Cleanup(func() {
			teardown(model)
		})
	})
	t.Run("Should capture M2 migration", func(t *testing.T) {
		t.Skip()
		esm := stateManager.NewEigenStateManager(l, grm)

		originBlockNumber := uint64(100)

		block := storage.Block{
			Number: originBlockNumber,
			Hash:   "some hash",
		}
		res := grm.Model(storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		transaction := storage.Transaction{
			BlockNumber:      block.Number,
			TransactionHash:  "0x5ff283cb420cdf950036d538e2223d5b504b875828f6e0d243002f429da6faa2",
			TransactionIndex: big.NewInt(200).Uint64(),
			FromAddress:      "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78",
		}
		res = grm.Model(storage.Transaction{}).Create(&transaction)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		// setup M1 withdrawal WithdrawalQueued (has root) and N many ShareWithdrawalQueued events (staker, strategy, shares)
		shareWithdrawalQueued := storage.TransactionLog{
			TransactionHash:  "0x5ff283cb420cdf950036d538e2223d5b504b875828f6e0d243002f429da6faa2",
			TransactionIndex: big.NewInt(200).Uint64(),
			BlockNumber:      originBlockNumber,
			Address:          cfg.GetContractsMapForChain().StrategyManager,
			Arguments:        `[{"Name": "depositor", "Type": "address", "Value": null, "Indexed": false}, {"Name": "nonce", "Type": "uint96", "Value": null, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": null, "Indexed": false}, {"Name": "shares", "Type": "uint256", "Value": null, "Indexed": false}]`,
			EventName:        "ShareWithdrawalQueued",
			LogIndex:         big.NewInt(1).Uint64(),
			OutputData:       `{"nonce": 0, "shares": 246393621132195985, "strategy": "0x298afb19a105d59e74658c4c334ff360bade6dd2", "depositor": "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&shareWithdrawalQueued)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		withdrawalQueued := storage.TransactionLog{
			TransactionHash:  "0x5ff283cb420cdf950036d538e2223d5b504b875828f6e0d243002f429da6faa2",
			TransactionIndex: big.NewInt(200).Uint64(),
			BlockNumber:      originBlockNumber,
			Address:          cfg.GetContractsMapForChain().StrategyManager,
			Arguments:        `[{"Name": "depositor", "Type": "address", "Value": null, "Indexed": false}, {"Name": "nonce", "Type": "uint96", "Value": null, "Indexed": false}, {"Name": "withdrawer", "Type": "address", "Value": null, "Indexed": false}, {"Name": "delegatedAddress", "Type": "address", "Value": null, "Indexed": false}, {"Name": "withdrawalRoot", "Type": "bytes32", "Value": null, "Indexed": false}]`,
			EventName:        "WithdrawalQueued",
			LogIndex:         big.NewInt(2).Uint64(),
			OutputData:       `{"nonce": 0, "depositor": "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", "withdrawer": "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", "withdrawalRoot": [31, 200, 156, 159, 43, 41, 112, 204, 139, 225, 142, 72, 58, 63, 194, 149, 59, 254, 218, 227, 162, 25, 237, 7, 103, 240, 24, 255, 31, 152, 236, 84], "delegatedAddress": "0x0000000000000000000000000000000000000000"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&withdrawalQueued)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		blockNumber := uint64(200)
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(300).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			Arguments:        `[{"Name": "oldWithdrawalRoot", "Type": "bytes32", "Value": ""}, {"Name": "newWithdrawalRoot", "Type": "bytes32", "Value": ""}]`,
			EventName:        "WithdrawalMigrated",
			LogIndex:         big.NewInt(600).Uint64(),
			OutputData:       `{"newWithdrawalRoot": [218, 200, 138, 86, 38, 9, 156, 119, 73, 13, 168, 40, 209, 43, 238, 83, 234, 177, 230, 73, 120, 205, 255, 143, 255, 216, 51, 209, 137, 100, 163, 233], "oldWithdrawalRoot": [31, 200, 156, 159, 43, 41, 112, 204, 139, 225, 142, 72, 58, 63, 194, 149, 59, 254, 218, 227, 162, 25, 237, 7, 103, 240, 24, 255, 31, 152, 236, 84]}`,
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

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		shareDiff := diffs.StateDiffs[0].Event.(*StakerShareDeltas)
		assert.Equal(t, "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", shareDiff.Staker)
		assert.Equal(t, "0x298afb19a105d59e74658c4c334ff360bade6dd2", shareDiff.Strategy)
		assert.Equal(t, "246393621132195985", shareDiff.Shares)

		preparedChange, err := model.prepareState(blockNumber)
		assert.Nil(t, err)
		assert.Equal(t, "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", preparedChange[0].Staker)
		assert.Equal(t, "0x298afb19a105d59e74658c4c334ff360bade6dd2", preparedChange[0].Strategy)
		assert.Equal(t, "246393621132195985", preparedChange[0].Shares.String())

		err = model.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		query := `select * from staker_shares where block_number = ?`
		results := []*StakerShares{}
		res = model.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)
		assert.Equal(t, 1, len(results))

		t.Cleanup(func() {
			teardown(model)
		})
	})
	t.Run("Should handle an M1 withdrawal and migration to M2 correctly", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		originBlockNumber := uint64(101)
		originTxHash := "0x5ff283cb420cdf950036d538e2223d5b504b875828f6e0d243002f429da6faa3"

		block := storage.Block{
			Number: originBlockNumber,
			Hash:   "some hash",
		}
		res := grm.Model(storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		transaction := storage.Transaction{
			BlockNumber:      block.Number,
			TransactionHash:  originTxHash,
			TransactionIndex: big.NewInt(200).Uint64(),
			FromAddress:      "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78",
		}
		res = grm.Model(storage.Transaction{}).Create(&transaction)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		// Insert the M1 withdrawal since we'll need it later
		shareWithdrawalQueued := storage.TransactionLog{
			TransactionHash:  originTxHash,
			TransactionIndex: big.NewInt(1).Uint64(),
			BlockNumber:      originBlockNumber,
			Address:          cfg.GetContractsMapForChain().StrategyManager,
			Arguments:        `[{"Name": "depositor", "Type": "address", "Value": null, "Indexed": false}, {"Name": "nonce", "Type": "uint96", "Value": null, "Indexed": false}, {"Name": "strategy", "Type": "address", "Value": null, "Indexed": false}, {"Name": "shares", "Type": "uint256", "Value": null, "Indexed": false}]`,
			EventName:        "ShareWithdrawalQueued",
			LogIndex:         big.NewInt(1).Uint64(),
			OutputData:       `{"nonce": 0, "shares": 246393621132195985, "strategy": "0x298afb19a105d59e74658c4c334ff360bade6dd2", "depositor": "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&shareWithdrawalQueued)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		// init processing for the M1 withdrawal
		err = model.SetupStateForBlock(originBlockNumber)
		assert.Nil(t, err)

		change, err := model.HandleStateChange(&shareWithdrawalQueued)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		diffs := change.(*AccumulatedStateDiffs)

		assert.Equal(t, 1, len(diffs.StateDiffs))

		shareDiff := diffs.StateDiffs[0].Event.(*StakerShareDeltas)
		assert.Equal(t, "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", shareDiff.Staker)
		assert.Equal(t, "0x298afb19a105d59e74658c4c334ff360bade6dd2", shareDiff.Strategy)
		assert.Equal(t, "-246393621132195985", shareDiff.Shares)

		deltas, ok := model.diffAccumulator[originBlockNumber]
		assert.True(t, ok)
		assert.NotNil(t, deltas)
		assert.Equal(t, 1, len(deltas))

		delta := deltas[0].Event.(*StakerShareDeltas)
		assert.Equal(t, "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", delta.Staker)
		assert.Equal(t, "0x298afb19a105d59e74658c4c334ff360bade6dd2", delta.Strategy)
		assert.Equal(t, "-246393621132195985", delta.Shares)

		// Insert the other half of the M1 event that captures the withdrawalRoot associated with the M1 withdrawal
		// No need to process this event, we just need it to be present in the DB
		withdrawalQueued := storage.TransactionLog{
			TransactionHash:  originTxHash,
			TransactionIndex: big.NewInt(200).Uint64(),
			BlockNumber:      originBlockNumber,
			Address:          cfg.GetContractsMapForChain().StrategyManager,
			Arguments:        `[{"Name": "depositor", "Type": "address", "Value": null, "Indexed": false}, {"Name": "nonce", "Type": "uint96", "Value": null, "Indexed": false}, {"Name": "withdrawer", "Type": "address", "Value": null, "Indexed": false}, {"Name": "delegatedAddress", "Type": "address", "Value": null, "Indexed": false}, {"Name": "withdrawalRoot", "Type": "bytes32", "Value": null, "Indexed": false}]`,
			EventName:        "WithdrawalQueued",
			LogIndex:         big.NewInt(2).Uint64(),
			OutputData:       `{"nonce": 0, "depositor": "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", "withdrawer": "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", "withdrawalRoot": [31, 200, 156, 159, 43, 41, 112, 204, 139, 225, 142, 72, 58, 63, 194, 149, 59, 254, 218, 227, 162, 25, 237, 7, 103, 240, 24, 255, 31, 152, 236, 84], "delegatedAddress": "0x0000000000000000000000000000000000000000"}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}
		res = grm.Model(storage.TransactionLog{}).Create(&withdrawalQueued)
		if res.Error != nil {
			t.Fatal(res.Error)
		}

		change, err = model.HandleStateChange(&withdrawalQueued)
		assert.Nil(t, err)
		assert.Nil(t, change.(*AccumulatedStateDiffs).StateDiffs) // should be nil since the handler doesnt care about this event

		err = model.CommitFinalState(originBlockNumber)
		assert.Nil(t, err)

		// verify the M1 withdrawal was processed correctly
		query := `select * from staker_shares where block_number = ?`
		results := []*StakerShares{}
		res = model.DB.Raw(query, originBlockNumber).Scan(&results)

		assert.Nil(t, res.Error)
		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", results[0].Staker)
		assert.Equal(t, "0x298afb19a105d59e74658c4c334ff360bade6dd2", results[0].Strategy)
		assert.Equal(t, "-246393621132195985", results[0].Shares)

		// setup M2 migration
		blockNumber := uint64(102)
		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		err = createBlock(model, blockNumber)
		assert.Nil(t, err)

		// M2 WithdrawalQueued comes before the M2 WithdrawalMigrated event
		log := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(1).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			Arguments:        `[{"Name": "withdrawalRoot", "Type": "bytes32", "Value": ""}, {"Name": "withdrawal", "Type": "(address,address,address,uint256,uint32,address[],uint256[])", "Value": ""}]`,
			EventName:        "WithdrawalQueued",
			LogIndex:         big.NewInt(600).Uint64(),
			OutputData:       `{"withdrawal": {"nonce": 0, "shares": [246393621132195985], "staker": "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", "startBlock": 1215690, "strategies": ["0x298afb19a105d59e74658c4c334ff360bade6dd2"], "withdrawer": "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", "delegatedTo": "0x2177dee1f66d6dbfbf517d9c4f316024c6a21aeb"}, "withdrawalRoot": [24, 23, 49, 137, 14, 63, 119, 12, 234, 225, 63, 35, 109, 249, 112, 24, 241, 118, 212, 52, 22, 107, 202, 56, 105, 37, 68, 47, 169, 23, 142, 135]}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		change, err = model.HandleStateChange(&log)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		diffs = change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		shareDiff = diffs.StateDiffs[0].Event.(*StakerShareDeltas)
		assert.Equal(t, "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", shareDiff.Staker)
		assert.Equal(t, "0x298afb19a105d59e74658c4c334ff360bade6dd2", shareDiff.Strategy)
		assert.Equal(t, "-246393621132195985", shareDiff.Shares)

		// M2 WithdrawalMigrated event. Typically occurs in the same block as the M2 WithdrawalQueued event
		withdrawalMigratedLog := storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(2).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().DelegationManager,
			Arguments:        `[{"Name": "oldWithdrawalRoot", "Type": "bytes32", "Value": ""}, {"Name": "newWithdrawalRoot", "Type": "bytes32", "Value": ""}]`,
			EventName:        "WithdrawalMigrated",
			LogIndex:         big.NewInt(600).Uint64(),
			OutputData:       `{"newWithdrawalRoot": [24, 23, 49, 137, 14, 63, 119, 12, 234, 225, 63, 35, 109, 249, 112, 24, 241, 118, 212, 52, 22, 107, 202, 56, 105, 37, 68, 47, 169, 23, 142, 135], "oldWithdrawalRoot": [31, 200, 156, 159, 43, 41, 112, 204, 139, 225, 142, 72, 58, 63, 194, 149, 59, 254, 218, 227, 162, 25, 237, 7, 103, 240, 24, 255, 31, 152, 236, 84]}`,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		change, err = model.HandleStateChange(&withdrawalMigratedLog)
		assert.Nil(t, err)
		assert.NotNil(t, change)

		diffs = change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		shareDiff = diffs.StateDiffs[0].Event.(*StakerShareDeltas)
		assert.Equal(t, "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", shareDiff.Staker)
		assert.Equal(t, "0x298afb19a105d59e74658c4c334ff360bade6dd2", shareDiff.Strategy)
		assert.Equal(t, "246393621132195985", shareDiff.Shares)

		deltas = model.diffAccumulator[originBlockNumber]
		assert.NotNil(t, deltas)
		assert.Equal(t, 1, len(deltas))

		delta = deltas[0].Event.(*StakerShareDeltas)
		assert.Equal(t, "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", delta.Staker)
		assert.Equal(t, "0x298afb19a105d59e74658c4c334ff360bade6dd2", delta.Strategy)
		assert.Equal(t, "-246393621132195985", delta.Shares)

		err = model.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		// Get the state at the new block and verify the shares amount is correct
		query = `
			select * from staker_shares
			where block_number = ?
		`
		results = []*StakerShares{}
		res = model.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0x9c01148c464cf06d135ad35d3d633ab4b46b9b78", results[0].Staker)
		assert.Equal(t, "0x298afb19a105d59e74658c4c334ff360bade6dd2", results[0].Strategy)
		assert.Equal(t, "-246393621132195985", results[0].Shares)
		assert.Equal(t, blockNumber, results[0].BlockNumber)

		t.Cleanup(func() {
			teardown(model)
		})
	})
	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
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

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		shareDiff := diffs.StateDiffs[0].Event.(*StakerShareDeltas)
		assert.Equal(t, "-50000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xd523267698c81a372191136e477fdebfa33d9fb4", shareDiff.Strategy)

		teardown(model)
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

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 2, len(diffs.StateDiffs))

		shareDiff := diffs.StateDiffs[0].Event.(*StakerShareDeltas)
		assert.Equal(t, "-50000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xd523267698c81a372191136e477fdebfa33d9fb4", shareDiff.Strategy)

		shareDiff = diffs.StateDiffs[1].Event.(*StakerShareDeltas)
		assert.Equal(t, "-100000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xe523267698c81a372191136e477fdebfa33d9fb5", shareDiff.Strategy)

		teardown(model)
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

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		shareDiff := diffs.StateDiffs[0].Event.(*StakerShareDeltas)
		assert.Equal(t, "-50000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xd523267698c81a372191136e477fdebfa33d9fb4", shareDiff.Strategy)

		teardown(model)
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

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 2, len(diffs.StateDiffs))

		shareDiff := diffs.StateDiffs[0].Event.(*StakerShareDeltas)
		assert.Equal(t, "-50000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xd523267698c81a372191136e477fdebfa33d9fb4", shareDiff.Strategy)

		shareDiff = diffs.StateDiffs[1].Event.(*StakerShareDeltas)
		assert.Equal(t, "-100000000000000", shareDiff.Shares)
		assert.Equal(t, strings.ToLower("0x3c42cd72639e3e8d11ab8d0072cc13bd5d8aa83c"), shareDiff.Staker)
		assert.Equal(t, "0xe523267698c81a372191136e477fdebfa33d9fb5", shareDiff.Strategy)

		teardown(model)
	})

	t.Run("Should capture delegate, deposit, slash in same block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)

		change, err := processSlashing(model, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		slashDiff := diffs.StateDiffs[0].Event.(*SlashDiff)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.Operator)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadsSlashed.String())

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		query := `
			select * from staker_shares
			where block_number = ?
		`
		results := []*StakerShares{}
		res := model.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "900000000000000000", results[0].Shares)

		teardown(model)
	})

	t.Run("Should capture many deposits and slash in same block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)
		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 301, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)
		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 401, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(2e18))
		assert.Nil(t, err)

		change, err := processSlashing(model, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		slashDiff := diffs.StateDiffs[0].Event.(*SlashDiff)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.Operator)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadsSlashed.String())

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		query := `
			select * from staker_shares
			where block_number = ?
			order by staker asc
		`
		results := []*StakerShares{}
		res := model.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 2, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "900000000000000000", results[0].Shares)

		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", results[1].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[1].Strategy)
		assert.Equal(t, "1800000000000000000", results[1].Shares)

		teardown(model)
	})

	t.Run("Should capture many deposits and slash in a different block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)
		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 301, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)
		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 401, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(2e18))
		assert.Nil(t, err)

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		blockNumber = blockNumber + 1
		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := processSlashing(model, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		slashDiff := diffs.StateDiffs[0].Event.(*SlashDiff)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.Operator)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadsSlashed.String())

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		query := `
			select * from staker_shares
			where block_number = ?
			order by staker asc
		`
		results := []*StakerShares{}
		res := model.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 2, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "900000000000000000", results[0].Shares)

		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", results[1].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[1].Strategy)
		assert.Equal(t, "1800000000000000000", results[1].Shares)

		teardown(model)
	})

	t.Run("Should not slash delegated staker in a different strategy for a deposit in same block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x1234567890abcdef1234567890abcdef12345678", big.NewInt(1e18))
		assert.Nil(t, err)

		change, err := processSlashing(model, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		slashDiff := diffs.StateDiffs[0].Event.(*SlashDiff)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.Operator)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadsSlashed.String())

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		query := `
			select * from staker_shares
			where block_number = ?
		`
		results := []*StakerShares{}
		res := model.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", results[0].Strategy)
		assert.Equal(t, "1000000000000000000", results[0].Shares)

		teardown(model)
	})

	t.Run("Should not slash delegated staker in a different strategy deposited in previous block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x1234567890abcdef1234567890abcdef12345678", big.NewInt(1e18))
		assert.Nil(t, err)

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		blockNumber = blockNumber + 1
		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := processSlashing(model, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		slashDiff := diffs.StateDiffs[0].Event.(*SlashDiff)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.Operator)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadsSlashed.String())

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		query := `
			select * from staker_shares
		`
		results := []*StakerShares{}
		res := model.DB.Raw(query).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", results[0].Strategy)
		assert.Equal(t, "1000000000000000000", results[0].Shares)
		assert.Equal(t, blockNumber-1, results[0].BlockNumber)

		teardown(model)
	})

	t.Run("Should not slash deposit after slashing in same block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		blockNumber = blockNumber + 1
		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := processSlashing(model, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		slashDiff := diffs.StateDiffs[0].Event.(*SlashDiff)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.Operator)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadsSlashed.String())

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 600, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		query := `
			select * from staker_shares
			where block_number = ?
		`
		results := []*StakerShares{}
		res := model.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "1900000000000000000", results[0].Shares)

		teardown(model)
	})

	t.Run("Should not slash deposit after slashing in same block", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		blockNumber = blockNumber + 1
		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := processSlashing(model, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		slashDiff := diffs.StateDiffs[0].Event.(*SlashDiff)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.Operator)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadsSlashed.String())

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 600, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(2e18))
		assert.Nil(t, err)

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		query := `
			select * from staker_shares
			where block_number = ?
		`
		results := []*StakerShares{}
		res := model.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "2900000000000000000", results[0].Shares)

		teardown(model)
	})

	t.Run("Should process slashing for several strategies correctly", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 301, "0x4444444444444444444444444444444444444444", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 401, "0x4444444444444444444444444444444444444444", "0x1234567890abcdef1234567890abcdef12345678", big.NewInt(2e18))
		assert.Nil(t, err)

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		blockNumber = blockNumber + 1
		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := processSlashing(model, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", "0x1234567890abcdef1234567890abcdef12345678"}, []*big.Int{big.NewInt(1e17), big.NewInt(9e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 2, len(diffs.StateDiffs))

		slashDiff := diffs.StateDiffs[0].Event.(*SlashDiff)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.Operator)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadsSlashed.String())

		slashDiff = diffs.StateDiffs[1].Event.(*SlashDiff)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.Operator)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", slashDiff.Strategy)
		assert.Equal(t, "900000000000000000", slashDiff.WadsSlashed.String())

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		query := `
			select * from staker_shares
			where block_number = ?
			order by staker asc
		`
		results := []*StakerShares{}
		res := model.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 2, len(results))
		assert.Equal(t, "0x4444444444444444444444444444444444444444", results[0].Staker)
		assert.Equal(t, "0x1234567890abcdef1234567890abcdef12345678", results[0].Strategy)
		assert.Equal(t, "200000000000000000", results[0].Shares)

		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[1].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[1].Strategy)
		assert.Equal(t, "900000000000000000", results[1].Shares)

		teardown(model)
	})

	t.Run("Should handle a full slashing", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(1e18))
		assert.Nil(t, err)

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		blockNumber = blockNumber + 1
		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := processSlashing(model, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e18)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		slashDiff := diffs.StateDiffs[0].Event.(*SlashDiff)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.Operator)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "1000000000000000000", slashDiff.WadsSlashed.String())

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		query := `
			select * from staker_shares
			where block_number = ?
		`
		results := []*StakerShares{}
		res := model.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0xaf6fb48ac4a60c61a64124ce9dc28f508dc8de8d", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "0", results[0].Shares)

		teardown(model)
	})

	t.Run("Should slashing when staker has 0 shares", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)
		blockNumber := uint64(200)

		delegationModel, err := stakerDelegations.NewStakerDelegationsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = delegationModel.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDelegation(delegationModel, cfg.GetContractsMapForChain().DelegationManager, blockNumber, 300, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", "0xbde83df53bc7d159700e966ad5d21e8b7c619459")
		assert.Nil(t, err)

		err = delegationModel.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		model, err := NewStakerSharesModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		_, err = processDeposit(model, cfg.GetContractsMapForChain().StrategyManager, blockNumber, 400, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", big.NewInt(0))
		assert.Nil(t, err)

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		blockNumber = blockNumber + 1
		err = model.SetupStateForBlock(blockNumber)
		assert.Nil(t, err)

		change, err := processSlashing(model, cfg.GetContractsMapForChain().AllocationManager, blockNumber, 500, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", []string{"0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"}, []*big.Int{big.NewInt(1e17)})
		assert.Nil(t, err)

		diffs := change.(*AccumulatedStateDiffs)
		assert.Equal(t, 1, len(diffs.StateDiffs))

		slashDiff := diffs.StateDiffs[0].Event.(*SlashDiff)
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", slashDiff.Operator)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", slashDiff.Strategy)
		assert.Equal(t, "100000000000000000", slashDiff.WadsSlashed.String())

		err = createBlockAndCommitFinalState(model, blockNumber)
		assert.Nil(t, err)

		query := `
			select * from staker_shares
			where block_number = ?
		`
		results := []*StakerShares{}
		res := model.DB.Raw(query, blockNumber).Scan(&results)
		assert.Nil(t, res.Error)

		assert.Equal(t, 1, len(results))
		assert.Equal(t, "0xbde83df53bc7d159700e966ad5d21e8b7c619459", results[0].Staker)
		assert.Equal(t, "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", results[0].Strategy)
		assert.Equal(t, "0", results[0].Shares)

		teardown(model)
	})

}

func createBlockAndCommitFinalState(model *StakerSharesModel, blockNumber uint64) error {
	block := storage.Block{
		Number: blockNumber,
		Hash:   "some hash",
	}
	res := model.DB.Model(storage.Block{}).Create(&block)
	if res.Error != nil {
		return res.Error
	}

	return model.CommitFinalState(blockNumber)
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

func processSlashing(stakerSharesModel *StakerSharesModel, allocationManager string, blockNumber, logIndex uint64, operator string, strategies []string, wadsSlashed []*big.Int) (interface{}, error) {
	wadsSlashedJson := make([]json.Number, len(wadsSlashed))
	for i, wad := range wadsSlashed {
		wadsSlashedJson[i] = json.Number(wad.String())
	}

	operatorSlashedEvent := operatorSlashedOutputData{
		Operator:    operator,
		Strategies:  strategies,
		WadsSlashed: wadsSlashedJson,
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
