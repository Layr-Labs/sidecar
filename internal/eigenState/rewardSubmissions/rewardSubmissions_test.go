package rewardSubmissions

import (
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/eigenStateModel"
	"github.com/Layr-Labs/go-sidecar/internal/eigenState/stateManager"
	"github.com/Layr-Labs/go-sidecar/internal/logger"
	"github.com/Layr-Labs/go-sidecar/internal/sqlite/migrations"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"github.com/Layr-Labs/go-sidecar/internal/tests"
	"github.com/Layr-Labs/go-sidecar/internal/tests/sqlite"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup() (
	*config.Config,
	*gorm.DB,
	*zap.Logger,
	error,
) {
	cfg := tests.GetConfig()
	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	db, err := sqlite.GetInMemorySqliteDatabaseConnection(l)
	if err != nil {
		panic(err)
	}
	sqliteMigrator := migrations.NewSqliteMigrator(db, l)
	if err := sqliteMigrator.MigrateAll(); err != nil {
		l.Sugar().Fatalw("Failed to migrate", "error", err)
	}

	return cfg, db, l, err
}

func teardown(model *eigenStateModel.EigenStateModel) {
	queries := []string{
		`delete from reward_submissions`,
		`delete from blocks`,
	}
	for _, query := range queries {
		res := model.DB().Exec(query)
		if res.Error != nil {
			fmt.Printf("Failed to run query: %v\n", res.Error)
		}
	}
}

func createBlock(model *eigenStateModel.EigenStateModel, blockNumber uint64) error {
	block := &storage.Block{
		Number:    blockNumber,
		Hash:      "some hash",
		BlockTime: time.Now().Add(time.Hour * time.Duration(blockNumber)),
	}
	res := model.DB().Model(&storage.Block{}).Create(block)
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func Test_RewardSubmissions(t *testing.T) {
	cfg, grm, l, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test each event type", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)

		model, err := NewRewardSubmissionsModel(esm, grm, l, cfg)

		submissionCounter := 0

		t.Run("Handle a range payment submission", func(t *testing.T) {
			blockNumber := uint64(100)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
				Arguments:        `[{"Name": "avs", "Type": "address", "Value": "0x00526A07855f743964F05CccAeCcf7a9E34847fF"}, {"Name": "paymentNonce", "Type": "uint256", "Value": "0x0000000000000000000000000000000000000000"}, {"Name": "rangePaymentHash", "Type": "bytes32", "Value": "0x58959fBe6661daEA647E20dF7c6d2c7F0d2215fB"}, {"Name": "rangePayment", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": ""}]`,
				EventName:        "RangePaymentCreated",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"rangePayment": {"token": "0x94373a4919b3240d86ea41593d5eba789fef3848", "amount": 50000000000000000000, "duration": 2419200, "startTimestamp": 1712188800, "strategiesAndMultipliers": [{"strategy": "0x3c28437e610fb099cc3d6de4d9c707dfacd308ae", "multiplier": 1000000000000000000}, {"strategy": "0x3cb1fd19cfb178c1098f2fc1e11090a0642b2314", "multiplier": 2000000000000000000}, {"strategy": "0x5c8b55722f421556a2aafb7a3ea63d4c3e514312", "multiplier": 3000000000000000000}, {"strategy": "0x6dc6ce589f852f96ac86cb160ab0b15b9f56dedd", "multiplier": 4500000000000000000}, {"strategy": "0x87f6c7d24b109919eb38295e3f8298425e6331d9", "multiplier": 500000000000000000}, {"strategy": "0xd523267698c81a372191136e477fdebfa33d9fb4", "multiplier": 8000000000000000000}, {"strategy": "0xdccf401fd121d8c542e96bc1d0078884422afad2", "multiplier": 5000000000000000000}]}}`,
			}

			err = model.InitBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			strategiesAndMultipliers := []struct {
				Strategy   string
				Multiplier string
			}{
				{"0x3c28437e610fb099cc3d6de4d9c707dfacd308ae", "1000000000000000000"},
				{"0x3cb1fd19cfb178c1098f2fc1e11090a0642b2314", "2000000000000000000"},
				{"0x5c8b55722f421556a2aafb7a3ea63d4c3e514312", "3000000000000000000"},
				{"0x6dc6ce589f852f96ac86cb160ab0b15b9f56dedd", "4500000000000000000"},
				{"0x87f6c7d24b109919eb38295e3f8298425e6331d9", "500000000000000000"},
				{"0xd523267698c81a372191136e477fdebfa33d9fb4", "8000000000000000000"},
				{"0xdccf401fd121d8c542e96bc1d0078884422afad2", "5000000000000000000"},
			}

			submissions := change.([]*RewardSubmission)
			assert.Equal(t, len(strategiesAndMultipliers), len(submissions))

			for i, submission := range submissions {
				assert.Equal(t, strings.ToLower("0x00526A07855f743964F05CccAeCcf7a9E34847fF"), strings.ToLower(submission.Avs))
				assert.Equal(t, strings.ToLower("0x94373a4919b3240d86ea41593d5eba789fef3848"), strings.ToLower(submission.Token))
				assert.Equal(t, strings.ToLower("0x58959fBe6661daEA647E20dF7c6d2c7F0d2215fB"), strings.ToLower(submission.RewardHash))
				assert.Equal(t, "50000000000000000000", submission.Amount)
				assert.Equal(t, uint64(2419200), submission.Duration)
				assert.Equal(t, int64(1712188800), submission.StartTimestamp.Unix())
				assert.Equal(t, int64(2419200+1712188800), submission.EndTimestamp.Unix())

				assert.Equal(t, strings.ToLower(strategiesAndMultipliers[i].Strategy), strings.ToLower(submission.Strategy))
				assert.Equal(t, strategiesAndMultipliers[i].Multiplier, submission.Multiplier)
			}

			err = model.CommitFinalState(blockNumber)
			assert.Nil(t, err)

			rewards := make([]*RewardSubmission, 0)
			query := `select * from reward_submissions where block_number = ?`
			res := model.DB().Raw(query, blockNumber).Scan(&rewards)
			assert.Nil(t, res.Error)
			assert.Equal(t, len(strategiesAndMultipliers), len(rewards))

			submissionCounter += len(rewards)

			stateRoot, err := model.GenerateStateRoot(blockNumber)
			assert.Nil(t, err)
			assert.NotNil(t, stateRoot)
			assert.True(t, len(stateRoot) > 0)

			t.Cleanup(func() {
				teardown(model)
			})
		})

		t.Run("Handle a range payment for all submission", func(t *testing.T) {
			blockNumber := uint64(101)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
				Arguments:        `[{"Name": "submitter", "Type": "address", "Value": "0x00526A07855f743964F05CccAeCcf7a9E34847fF"}, {"Name": "paymentNonce", "Type": "uint256", "Value": "0x0000000000000000000000000000000000000001"}, {"Name": "rangePaymentHash", "Type": "bytes32", "Value": "0x69193C881C4BfA9015F1E9B2631e31238BedB93e"}, {"Name": "rangePayment", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": ""}]`,
				EventName:        "RangePaymentForAllCreated",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"rangePayment": {"token": "0x3f1c547b21f65e10480de3ad8e19faac46c95034", "amount": 11000000000000000000, "duration": 2419200, "startTimestamp": 1713398400, "strategiesAndMultipliers": [{"strategy": "0x5c8b55722f421556a2aafb7a3ea63d4c3e514312", "multiplier": 1000000000000000000}, {"strategy": "0x7fa77c321bf66e42eabc9b10129304f7f90c5585", "multiplier": 2000000000000000000}, {"strategy": "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", "multiplier": 3000000000000000000}, {"strategy": "0xd523267698c81a372191136e477fdebfa33d9fb4", "multiplier": 4500000000000000000}]}}`,
			}

			err = model.InitBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			strategiesAndMultipliers := []struct {
				Strategy   string
				Multiplier string
			}{
				{"0x5c8b55722f421556a2aafb7a3ea63d4c3e514312", "1000000000000000000"},
				{"0x7fa77c321bf66e42eabc9b10129304f7f90c5585", "2000000000000000000"},
				{"0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", "3000000000000000000"},
				{"0xd523267698c81a372191136e477fdebfa33d9fb4", "4500000000000000000"},
			}

			submissions := change.([]*RewardSubmission)
			assert.Equal(t, len(strategiesAndMultipliers), len(submissions))

			for i, submission := range submissions {
				assert.Equal(t, strings.ToLower("0x00526A07855f743964F05CccAeCcf7a9E34847fF"), strings.ToLower(submission.Avs))
				assert.Equal(t, strings.ToLower("0x3f1c547b21f65e10480de3ad8e19faac46c95034"), strings.ToLower(submission.Token))
				assert.Equal(t, strings.ToLower("0x69193C881C4BfA9015F1E9B2631e31238BedB93e"), strings.ToLower(submission.RewardHash))
				assert.Equal(t, "11000000000000000000", submission.Amount)
				assert.Equal(t, uint64(2419200), submission.Duration)
				assert.Equal(t, int64(1713398400), submission.StartTimestamp.Unix())
				assert.Equal(t, int64(2419200+1713398400), submission.EndTimestamp.Unix())

				assert.Equal(t, strings.ToLower(strategiesAndMultipliers[i].Strategy), strings.ToLower(submission.Strategy))
				assert.Equal(t, strategiesAndMultipliers[i].Multiplier, submission.Multiplier)
			}

			err = model.CommitFinalState(blockNumber)
			assert.Nil(t, err)

			rewards := make([]*RewardSubmission, 0)
			query := `select * from reward_submissions where block_number = ?`
			res := model.DB().Raw(query, blockNumber).Scan(&rewards)
			assert.Nil(t, res.Error)
			assert.Equal(t, len(strategiesAndMultipliers), len(rewards))

			submissionCounter += len(strategiesAndMultipliers)

			stateRoot, err := model.GenerateStateRoot(blockNumber)
			assert.Nil(t, err)
			assert.NotNil(t, stateRoot)
			assert.True(t, len(stateRoot) > 0)

			t.Cleanup(func() {
				teardown(model)
			})
		})

		t.Run("Handle a reward submission", func(t *testing.T) {
			blockNumber := uint64(102)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
				Arguments:        `[{"Name": "avs", "Type": "address", "Value": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "Indexed": true}, {"Name": "submissionNonce", "Type": "uint256", "Value": 0, "Indexed": true}, {"Name": "RewardSubmissionHash", "Type": "bytes32", "Value": "0x7402669fb2c8a0cfe8108acb8a0070257c77ec6906ecb07d97c38e8a5ddc66a9", "Indexed": true}, {"Name": "RewardSubmission", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": null, "Indexed": false}]`,
				EventName:        "AVSRewardSubmissionCreated",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"RewardSubmission": {"token": "0x0ddd9dc88e638aef6a8e42d0c98aaa6a48a98d24", "amount": 10000000000000000000000, "duration": 2419200, "startTimestamp": 1725494400, "strategiesAndMultipliers": [{"strategy": "0x5074dfd18e9498d9e006fb8d4f3fecdc9af90a2c", "multiplier": 1000000000000000000}]}}`,
			}

			err = model.InitBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			strategiesAndMultipliers := []struct {
				Strategy   string
				Multiplier string
			}{
				{"0x5074dfd18e9498d9e006fb8d4f3fecdc9af90a2c", "1000000000000000000"},
			}

			submissions := change.([]*RewardSubmission)
			assert.Equal(t, len(strategiesAndMultipliers), len(submissions))

			for i, submission := range submissions {
				assert.Equal(t, strings.ToLower("0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101"), strings.ToLower(submission.Avs))
				assert.Equal(t, strings.ToLower("0x0ddd9dc88e638aef6a8e42d0c98aaa6a48a98d24"), strings.ToLower(submission.Token))
				assert.Equal(t, strings.ToLower("0x7402669fb2c8a0cfe8108acb8a0070257c77ec6906ecb07d97c38e8a5ddc66a9"), strings.ToLower(submission.RewardHash))
				assert.Equal(t, "10000000000000000000000", submission.Amount)
				assert.Equal(t, uint64(2419200), submission.Duration)
				assert.Equal(t, int64(1725494400), submission.StartTimestamp.Unix())
				assert.Equal(t, int64(2419200+1725494400), submission.EndTimestamp.Unix())

				assert.Equal(t, strings.ToLower(strategiesAndMultipliers[i].Strategy), strings.ToLower(submission.Strategy))
				assert.Equal(t, strategiesAndMultipliers[i].Multiplier, submission.Multiplier)
			}

			err = model.CommitFinalState(blockNumber)
			assert.Nil(t, err)

			rewards := make([]*RewardSubmission, 0)
			query := `select * from reward_submissions where block_number = ?`
			res := model.DB().Raw(query, blockNumber).Scan(&rewards)
			assert.Nil(t, res.Error)
			assert.Equal(t, len(strategiesAndMultipliers), len(rewards))

			submissionCounter += len(strategiesAndMultipliers)

			stateRoot, err := model.GenerateStateRoot(blockNumber)
			assert.Nil(t, err)
			assert.NotNil(t, stateRoot)
			assert.True(t, len(stateRoot) > 0)

			t.Cleanup(func() {
				teardown(model)
			})
		})

		t.Run("Handle a reward submission for all", func(t *testing.T) {
			blockNumber := uint64(103)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
				Arguments:        `[{"Name": "submitter", "Type": "address", "Value": "0x66ae7d7c4d492e4e012b95977f14715b74498bc5", "Indexed": true}, {"Name": "submissionNonce", "Type": "uint256", "Value": 3, "Indexed": true}, {"Name": "RewardSubmissionHash", "Type": "bytes32", "Value": "0x99ebccb0f68eedbf3dff04c7773d6ff94fc439e0eebdd80918b3785ae8099f96", "Indexed": true}, {"Name": "RewardSubmission", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": null, "Indexed": false}]`,
				EventName:        "RewardSubmissionForAllCreated",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"RewardSubmission": {"token": "0x554c393923c753d146aa34608523ad7946b61662", "amount": 10000000000000000000, "duration": 1814400, "startTimestamp": 1717632000, "strategiesAndMultipliers": [{"strategy": "0xd523267698c81a372191136e477fdebfa33d9fb4", "multiplier": 1000000000000000000}, {"strategy": "0xdccf401fd121d8c542e96bc1d0078884422afad2", "multiplier": 2000000000000000000}]}}`,
			}

			err = model.InitBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			strategiesAndMultipliers := []struct {
				Strategy   string
				Multiplier string
			}{
				{"0xd523267698c81a372191136e477fdebfa33d9fb4", "1000000000000000000"},
				{"0xdccf401fd121d8c542e96bc1d0078884422afad2", "2000000000000000000"},
			}

			submissions := change.([]*RewardSubmission)
			assert.Equal(t, len(strategiesAndMultipliers), len(submissions))

			for i, submission := range submissions {
				assert.Equal(t, strings.ToLower("0x66ae7d7c4d492e4e012b95977f14715b74498bc5"), strings.ToLower(submission.Avs))
				assert.Equal(t, strings.ToLower("0x554c393923c753d146aa34608523ad7946b61662"), strings.ToLower(submission.Token))
				assert.Equal(t, strings.ToLower("0x99ebccb0f68eedbf3dff04c7773d6ff94fc439e0eebdd80918b3785ae8099f96"), strings.ToLower(submission.RewardHash))
				assert.Equal(t, "10000000000000000000", submission.Amount)
				assert.Equal(t, uint64(1814400), submission.Duration)
				assert.Equal(t, int64(1717632000), submission.StartTimestamp.Unix())
				assert.Equal(t, int64(1814400+1717632000), submission.EndTimestamp.Unix())

				assert.Equal(t, strings.ToLower(strategiesAndMultipliers[i].Strategy), strings.ToLower(submission.Strategy))
				assert.Equal(t, strategiesAndMultipliers[i].Multiplier, submission.Multiplier)
			}

			err = model.CommitFinalState(blockNumber)
			assert.Nil(t, err)

			rewards := make([]*RewardSubmission, 0)
			query := `select * from reward_submissions where block_number = ?`
			res := model.DB().Raw(query, blockNumber).Scan(&rewards)
			assert.Nil(t, res.Error)
			assert.Equal(t, len(strategiesAndMultipliers), len(rewards))

			submissionCounter += len(strategiesAndMultipliers)

			stateRoot, err := model.GenerateStateRoot(blockNumber)
			assert.Nil(t, err)
			assert.NotNil(t, stateRoot)
			assert.True(t, len(stateRoot) > 0)

			t.Cleanup(func() {
				teardown(model)
			})
		})

		t.Run("Handle a reward submission for all earners", func(t *testing.T) {
			blockNumber := uint64(104)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
				Arguments:        `[{"Name": "tokenHopper", "Type": "address", "Value": "0x8daae33cb2da8da23595adb19f271ef41e34bd8c", "Indexed": true}, {"Name": "submissionNonce", "Type": "uint256", "Value": 0, "Indexed": true}, {"Name": "RewardSubmissionHash", "Type": "bytes32", "Value": "0xeb2a1f63fd3274fa701ad2045c04b4f1274c6d7b5ff8a83d75d87e812b589c9c", "Indexed": true}, {"Name": "RewardSubmission", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": null, "Indexed": false}]`,
				EventName:        "RewardSubmissionForAllEarnersCreated",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"RewardSubmission": {"token": "0x3b78576f7d6837500ba3de27a60c7f594934027e", "amount": 321855128516280769230770, "duration": 604800, "startTimestamp": 1725494400, "strategiesAndMultipliers": [{"strategy": "0x43252609bff8a13dfe5e057097f2f45a24387a84", "multiplier": 1000000000000000000}]}}`,
			}

			err = model.InitBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			strategiesAndMultipliers := []struct {
				Strategy   string
				Multiplier string
			}{
				{"0x43252609bff8a13dfe5e057097f2f45a24387a84", "1000000000000000000"},
			}

			submissions := change.([]*RewardSubmission)
			assert.Equal(t, len(strategiesAndMultipliers), len(submissions))

			for i, submission := range submissions {
				assert.Equal(t, strings.ToLower("0x8daae33cb2da8da23595adb19f271ef41e34bd8c"), strings.ToLower(submission.Avs))
				assert.Equal(t, strings.ToLower("0x3b78576f7d6837500ba3de27a60c7f594934027e"), strings.ToLower(submission.Token))
				assert.Equal(t, strings.ToLower("0xeb2a1f63fd3274fa701ad2045c04b4f1274c6d7b5ff8a83d75d87e812b589c9c"), strings.ToLower(submission.RewardHash))
				assert.Equal(t, "321855128516280769230770", submission.Amount)
				assert.Equal(t, uint64(604800), submission.Duration)
				assert.Equal(t, int64(1725494400), submission.StartTimestamp.Unix())
				assert.Equal(t, int64(604800+1725494400), submission.EndTimestamp.Unix())

				assert.Equal(t, strings.ToLower(strategiesAndMultipliers[i].Strategy), strings.ToLower(submission.Strategy))
				assert.Equal(t, strategiesAndMultipliers[i].Multiplier, submission.Multiplier)
			}

			err = model.CommitFinalState(blockNumber)
			assert.Nil(t, err)

			rewards := make([]*RewardSubmission, 0)
			query := `select * from reward_submissions where block_number = ?`
			res := model.DB().Raw(query, blockNumber).Scan(&rewards)
			assert.Nil(t, res.Error)
			assert.Equal(t, len(strategiesAndMultipliers), len(rewards))

			submissionCounter += len(strategiesAndMultipliers)

			stateRoot, err := model.GenerateStateRoot(blockNumber)
			assert.Nil(t, err)
			assert.NotNil(t, stateRoot)
			assert.True(t, len(stateRoot) > 0)

			t.Cleanup(func() {
				teardown(model)
			})
		})

		t.Cleanup(func() {
			teardown(model)
		})
	})

	t.Run("multi-block test", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)

		model, err := NewRewardSubmissionsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		submissionCounter := 0

		blockNumber := uint64(100)
		// create first block
		if err := createBlock(model, blockNumber); err != nil {
			t.Fatal(err)
		}

		// First RangePaymentCreated
		log := &storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
			Arguments:        `[{"Name": "avs", "Type": "address", "Value": "0x00526A07855f743964F05CccAeCcf7a9E34847fF"}, {"Name": "paymentNonce", "Type": "uint256", "Value": "0x0000000000000000000000000000000000000000"}, {"Name": "rangePaymentHash", "Type": "bytes32", "Value": "0x58959fBe6661daEA647E20dF7c6d2c7F0d2215fB"}, {"Name": "rangePayment", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": ""}]`,
			EventName:        "RangePaymentCreated",
			LogIndex:         big.NewInt(12).Uint64(),
			OutputData:       `{"rangePayment": {"token": "0x94373a4919b3240d86ea41593d5eba789fef3848", "amount": 50000000000000000000, "duration": 2419200, "startTimestamp": 1712188800, "strategiesAndMultipliers": [{"strategy": "0x3c28437e610fb099cc3d6de4d9c707dfacd308ae", "multiplier": 1000000000000000000}, {"strategy": "0x3cb1fd19cfb178c1098f2fc1e11090a0642b2314", "multiplier": 2000000000000000000}, {"strategy": "0x5c8b55722f421556a2aafb7a3ea63d4c3e514312", "multiplier": 3000000000000000000}, {"strategy": "0x6dc6ce589f852f96ac86cb160ab0b15b9f56dedd", "multiplier": 4500000000000000000}, {"strategy": "0x87f6c7d24b109919eb38295e3f8298425e6331d9", "multiplier": 500000000000000000}, {"strategy": "0xd523267698c81a372191136e477fdebfa33d9fb4", "multiplier": 8000000000000000000}, {"strategy": "0xdccf401fd121d8c542e96bc1d0078884422afad2", "multiplier": 5000000000000000000}]}}`,
		}

		err = model.InitBlock(blockNumber)
		assert.Nil(t, err)

		isInteresting := model.IsInterestingLog(log)
		assert.True(t, isInteresting)

		change, err := model.HandleStateChange(log)
		assert.Nil(t, err)
		assert.NotNil(t, change)
		submissions := change.([]*RewardSubmission)

		err = model.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		submissionCounter += len(submissions)

		query := `select count(*) from reward_submissions where block_number = ?`
		var count int
		res := model.DB().Raw(query, blockNumber).Scan(&count)

		assert.Nil(t, res.Error)
		assert.Equal(t, submissionCounter, count)

		stateRoot, err := model.GenerateStateRoot(blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, stateRoot)
		assert.True(t, len(stateRoot) > 0)

		// -----

		blockNumber = uint64(101)
		// create block
		if err := createBlock(model, blockNumber); err != nil {
			t.Fatal(err)
		}

		// Second log: RangePaymentForAllCreated
		log = &storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
			Arguments:        `[{"Name": "submitter", "Type": "address", "Value": "0x00526A07855f743964F05CccAeCcf7a9E34847fF"}, {"Name": "paymentNonce", "Type": "uint256", "Value": "0x0000000000000000000000000000000000000001"}, {"Name": "rangePaymentHash", "Type": "bytes32", "Value": "0x69193C881C4BfA9015F1E9B2631e31238BedB93e"}, {"Name": "rangePayment", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": ""}]`,
			EventName:        "RangePaymentForAllCreated",
			LogIndex:         big.NewInt(12).Uint64(),
			OutputData:       `{"rangePayment": {"token": "0x3f1c547b21f65e10480de3ad8e19faac46c95034", "amount": 11000000000000000000, "duration": 2419200, "startTimestamp": 1713398400, "strategiesAndMultipliers": [{"strategy": "0x5c8b55722f421556a2aafb7a3ea63d4c3e514312", "multiplier": 1000000000000000000}, {"strategy": "0x7fa77c321bf66e42eabc9b10129304f7f90c5585", "multiplier": 2000000000000000000}, {"strategy": "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", "multiplier": 3000000000000000000}, {"strategy": "0xd523267698c81a372191136e477fdebfa33d9fb4", "multiplier": 4500000000000000000}]}}`,
		}

		err = model.InitBlock(blockNumber)
		assert.Nil(t, err)

		isInteresting = model.IsInterestingLog(log)
		assert.True(t, isInteresting)

		change, err = model.HandleStateChange(log)
		assert.Nil(t, err)
		assert.NotNil(t, change)
		submissions = change.([]*RewardSubmission)

		err = model.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		submissionCounter += len(submissions)

		stateRoot, err = model.GenerateStateRoot(blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, stateRoot)
		assert.True(t, len(stateRoot) > 0)

		query = `select count(*) from reward_submissions where block_number = ?`
		res = model.DB().Raw(query, blockNumber).Scan(&count)

		assert.Nil(t, res.Error)
		assert.Equal(t, submissionCounter, count)

		// -----

		blockNumber = uint64(102)
		// create block
		if err := createBlock(model, blockNumber); err != nil {
			t.Fatal(err)
		}

		log = &storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
			Arguments:        `[{"Name": "avs", "Type": "address", "Value": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "Indexed": true}, {"Name": "submissionNonce", "Type": "uint256", "Value": 0, "Indexed": true}, {"Name": "RewardSubmissionHash", "Type": "bytes32", "Value": "0x7402669fb2c8a0cfe8108acb8a0070257c77ec6906ecb07d97c38e8a5ddc66a9", "Indexed": true}, {"Name": "RewardSubmission", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": null, "Indexed": false}]`,
			EventName:        "AVSRewardSubmissionCreated",
			LogIndex:         big.NewInt(12).Uint64(),
			OutputData:       `{"RewardSubmission": {"token": "0x0ddd9dc88e638aef6a8e42d0c98aaa6a48a98d24", "amount": 10000000000000000000000, "duration": 2419200, "startTimestamp": 1725494400, "strategiesAndMultipliers": [{"strategy": "0x5074dfd18e9498d9e006fb8d4f3fecdc9af90a2c", "multiplier": 1000000000000000000}]}}`,
		}

		err = model.InitBlock(blockNumber)
		assert.Nil(t, err)

		isInteresting = model.IsInterestingLog(log)
		assert.True(t, isInteresting)

		change, err = model.HandleStateChange(log)
		assert.Nil(t, err)
		assert.NotNil(t, change)
		submissions = change.([]*RewardSubmission)

		err = model.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		submissionCounter += len(submissions)

		stateRoot, err = model.GenerateStateRoot(blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, stateRoot)
		assert.True(t, len(stateRoot) > 0)

		query = `select count(*) from reward_submissions where block_number = ?`
		res = model.DB().Raw(query, blockNumber).Scan(&count)

		assert.Nil(t, res.Error)
		assert.Equal(t, submissionCounter, count)

		// -----

		blockNumber = uint64(103)
		// create block
		if err := createBlock(model, blockNumber); err != nil {
			t.Fatal(err)
		}

		log = &storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
			Arguments:        `[{"Name": "submitter", "Type": "address", "Value": "0x66ae7d7c4d492e4e012b95977f14715b74498bc5", "Indexed": true}, {"Name": "submissionNonce", "Type": "uint256", "Value": 3, "Indexed": true}, {"Name": "RewardSubmissionHash", "Type": "bytes32", "Value": "0x99ebccb0f68eedbf3dff04c7773d6ff94fc439e0eebdd80918b3785ae8099f96", "Indexed": true}, {"Name": "RewardSubmission", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": null, "Indexed": false}]`,
			EventName:        "RewardSubmissionForAllCreated",
			LogIndex:         big.NewInt(12).Uint64(),
			OutputData:       `{"RewardSubmission": {"token": "0x554c393923c753d146aa34608523ad7946b61662", "amount": 10000000000000000000, "duration": 1814400, "startTimestamp": 1717632000, "strategiesAndMultipliers": [{"strategy": "0xd523267698c81a372191136e477fdebfa33d9fb4", "multiplier": 1000000000000000000}, {"strategy": "0xdccf401fd121d8c542e96bc1d0078884422afad2", "multiplier": 2000000000000000000}]}}`,
		}

		err = model.InitBlock(blockNumber)
		assert.Nil(t, err)

		isInteresting = model.IsInterestingLog(log)
		assert.True(t, isInteresting)

		change, err = model.HandleStateChange(log)
		assert.Nil(t, err)
		assert.NotNil(t, change)
		submissions = change.([]*RewardSubmission)

		err = model.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		submissionCounter += len(submissions)

		stateRoot, err = model.GenerateStateRoot(blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, stateRoot)
		assert.True(t, len(stateRoot) > 0)

		query = `select count(*) from reward_submissions where block_number = ?`
		res = model.DB().Raw(query, blockNumber).Scan(&count)

		assert.Nil(t, res.Error)
		assert.Equal(t, submissionCounter, count)

		t.Cleanup(func() {
			teardown(model)
		})
	})

	t.Run("single block, multiple events", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(l, grm)

		model, err := NewRewardSubmissionsModel(esm, grm, l, cfg)
		assert.Nil(t, err)

		submissionCounter := 0

		blockNumber := uint64(100)
		// create first block
		if err := createBlock(model, blockNumber); err != nil {
			t.Fatal(err)
		}

		err = model.InitBlock(blockNumber)
		assert.Nil(t, err)

		handleLog := func(log *storage.TransactionLog) {
			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)
			submissions := change.([]*RewardSubmission)

			submissionCounter += len(submissions)
		}

		// First RangePaymentCreated
		rangePaymentCreatedLog := &storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
			Arguments:        `[{"Name": "avs", "Type": "address", "Value": "0x00526A07855f743964F05CccAeCcf7a9E34847fF"}, {"Name": "paymentNonce", "Type": "uint256", "Value": "0x0000000000000000000000000000000000000000"}, {"Name": "rangePaymentHash", "Type": "bytes32", "Value": "0x58959fBe6661daEA647E20dF7c6d2c7F0d2215fB"}, {"Name": "rangePayment", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": ""}]`,
			EventName:        "RangePaymentCreated",
			LogIndex:         big.NewInt(12).Uint64(),
			OutputData:       `{"rangePayment": {"token": "0x94373a4919b3240d86ea41593d5eba789fef3848", "amount": 50000000000000000000, "duration": 2419200, "startTimestamp": 1712188800, "strategiesAndMultipliers": [{"strategy": "0x3c28437e610fb099cc3d6de4d9c707dfacd308ae", "multiplier": 1000000000000000000}, {"strategy": "0x3cb1fd19cfb178c1098f2fc1e11090a0642b2314", "multiplier": 2000000000000000000}, {"strategy": "0x5c8b55722f421556a2aafb7a3ea63d4c3e514312", "multiplier": 3000000000000000000}, {"strategy": "0x6dc6ce589f852f96ac86cb160ab0b15b9f56dedd", "multiplier": 4500000000000000000}, {"strategy": "0x87f6c7d24b109919eb38295e3f8298425e6331d9", "multiplier": 500000000000000000}, {"strategy": "0xd523267698c81a372191136e477fdebfa33d9fb4", "multiplier": 8000000000000000000}, {"strategy": "0xdccf401fd121d8c542e96bc1d0078884422afad2", "multiplier": 5000000000000000000}]}}`,
		}
		handleLog(rangePaymentCreatedLog)

		rangePaymentForAllLog := &storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
			Arguments:        `[{"Name": "submitter", "Type": "address", "Value": "0x00526A07855f743964F05CccAeCcf7a9E34847fF"}, {"Name": "paymentNonce", "Type": "uint256", "Value": "0x0000000000000000000000000000000000000001"}, {"Name": "rangePaymentHash", "Type": "bytes32", "Value": "0x69193C881C4BfA9015F1E9B2631e31238BedB93e"}, {"Name": "rangePayment", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": ""}]`,
			EventName:        "RangePaymentForAllCreated",
			LogIndex:         big.NewInt(12).Uint64(),
			OutputData:       `{"rangePayment": {"token": "0x3f1c547b21f65e10480de3ad8e19faac46c95034", "amount": 11000000000000000000, "duration": 2419200, "startTimestamp": 1713398400, "strategiesAndMultipliers": [{"strategy": "0x5c8b55722f421556a2aafb7a3ea63d4c3e514312", "multiplier": 1000000000000000000}, {"strategy": "0x7fa77c321bf66e42eabc9b10129304f7f90c5585", "multiplier": 2000000000000000000}, {"strategy": "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", "multiplier": 3000000000000000000}, {"strategy": "0xd523267698c81a372191136e477fdebfa33d9fb4", "multiplier": 4500000000000000000}]}}`,
		}
		handleLog(rangePaymentForAllLog)

		rewardSubmissionCreatedLog := &storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
			Arguments:        `[{"Name": "avs", "Type": "address", "Value": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "Indexed": true}, {"Name": "submissionNonce", "Type": "uint256", "Value": 0, "Indexed": true}, {"Name": "RewardSubmissionHash", "Type": "bytes32", "Value": "0x7402669fb2c8a0cfe8108acb8a0070257c77ec6906ecb07d97c38e8a5ddc66a9", "Indexed": true}, {"Name": "RewardSubmission", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": null, "Indexed": false}]`,
			EventName:        "AVSRewardSubmissionCreated",
			LogIndex:         big.NewInt(12).Uint64(),
			OutputData:       `{"RewardSubmission": {"token": "0x0ddd9dc88e638aef6a8e42d0c98aaa6a48a98d24", "amount": 10000000000000000000000, "duration": 2419200, "startTimestamp": 1725494400, "strategiesAndMultipliers": [{"strategy": "0x5074dfd18e9498d9e006fb8d4f3fecdc9af90a2c", "multiplier": 1000000000000000000}]}}`,
		}
		handleLog(rewardSubmissionCreatedLog)

		rewardsForAllLog := &storage.TransactionLog{
			TransactionHash:  "some hash",
			TransactionIndex: big.NewInt(100).Uint64(),
			BlockNumber:      blockNumber,
			Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
			Arguments:        `[{"Name": "submitter", "Type": "address", "Value": "0x002b273d4459b5636f971cc7be6443e95517d394", "Indexed": true}, {"Name": "submissionNonce", "Type": "uint256", "Value": 0, "Indexed": true}, {"Name": "RewardSubmissionHash", "Type": "bytes32", "Value": "0xcb5e9dfd219cc5500e88a349d8f072b77241475b3266a0f2c6cf29b1e09d3211", "Indexed": true}, {"Name": "RewardSubmission", "Type": "((address,uint96)[],address,uint256,uint32,uint32)", "Value": null, "Indexed": false}]`,
			EventName:        "RewardSubmissionForAllCreated",
			LogIndex:         big.NewInt(12).Uint64(),
			OutputData:       `{"RewardSubmission": {"token": "0xdeeeee2b48c121e6728ed95c860e296177849932", "amount": 1000000000000000000000000000, "duration": 5443200, "startTimestamp": 1717027200, "strategiesAndMultipliers": [{"strategy": "0x05037a81bd7b4c9e0f7b430f1f2a22c31a2fd943", "multiplier": 1000000000000000000}, {"strategy": "0x31b6f59e1627cefc9fa174ad03859fc337666af7", "multiplier": 1000000000000000000}, {"strategy": "0x3a8fbdf9e77dfc25d09741f51d3e181b25d0c4e0", "multiplier": 1000000000000000000}, {"strategy": "0x46281e3b7fdcacdba44cadf069a94a588fd4c6ef", "multiplier": 1000000000000000000}, {"strategy": "0x70eb4d3c164a6b4a5f908d4fbb5a9caffb66bab6", "multiplier": 1000000000000000000}, {"strategy": "0x7673a47463f80c6a3553db9e54c8cdcd5313d0ac", "multiplier": 1000000000000000000}, {"strategy": "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3", "multiplier": 1000000000000000000}, {"strategy": "0x80528d6e9a2babfc766965e0e26d5ab08d9cfaf9", "multiplier": 1000000000000000000}, {"strategy": "0x9281ff96637710cd9a5cacce9c6fad8c9f54631c", "multiplier": 1000000000000000000}, {"strategy": "0xaccc5a86732be85b5012e8614af237801636f8e5", "multiplier": 1000000000000000000}, {"strategy": "0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeeeeebeac0", "multiplier": 1000000000000000000}]}}`,
		}
		handleLog(rewardsForAllLog)

		// check that we're starting with 0 rows
		query := `select count(*) from reward_submissions`
		var count int
		res := model.DB().Raw(query).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, 0, count)

		// Commit the final state
		err = model.CommitFinalState(blockNumber)
		assert.Nil(t, err)

		// Generate the stateroot
		stateRoot, err := model.GenerateStateRoot(blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, stateRoot)
		assert.True(t, len(stateRoot) > 0)

		// Verify we have the right number of rows
		query = `select count(*) from reward_submissions where block_number = ?`
		res = model.DB().Raw(query, blockNumber).Scan(&count)
		assert.Nil(t, res.Error)
		assert.Equal(t, submissionCounter, count)

		t.Cleanup(func() {
			teardown(model)
		})
	})

}
