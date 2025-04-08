package operatorDirectedOperatorSetRewardSubmissions

import (
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

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
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func createBlock(model *OperatorDirectedOperatorSetRewardSubmissionsModel, blockNumber uint64) error {
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

func Test_OperatorDirectedOperatorSetRewardSubmissions(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	t.Run("Test each event type", func(t *testing.T) {
		esm := stateManager.NewEigenStateManager(nil, l, grm)

		model, err := NewOperatorDirectedOperatorSetRewardSubmissionsModel(esm, grm, l, cfg)

		submissionCounter := 0

		t.Run("Handle an operator directed operator set reward submission", func(t *testing.T) {
			blockNumber := uint64(102)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
				Arguments:        `[{"Name": "caller", "Type": "address", "Value": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "Indexed": true}, {"Name": "operatorDirectedRewardsSubmissionHash", "Type": "bytes32", "Value": "0x7402669fb2c8a0cfe8108acb8a0070257c77ec6906ecb07d97c38e8a5ddc66a9", "Indexed": true}, {"Name": "operatorSet", "Type": "tuple", "Value": {"avs": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "id": 1}, "Indexed": false}, {"Name": "submissionNonce", "Type": "uint256", "Value": 0, "Indexed": false}, {"Name": "operatorDirectedRewardsSubmission", "Type": "((address,uint96)[],address,(address,uint256)[],uint32,uint32,string)", "Value": null, "Indexed": false}]`,
				EventName:        "OperatorDirectedOperatorSetRewardsSubmissionCreated",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"operatorSet": {"avs": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "id": 1}, "submissionNonce": 0, "operatorDirectedRewardsSubmission": {"token": "0x0ddd9dc88e638aef6a8e42d0c98aaa6a48a98d24", "operatorRewards": [{"operator": "0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1", "amount": 30000000000000000000000}, {"operator": "0xF50Cba7a66b5E615587157e43286DaA7aF94009e", "amount": 40000000000000000000000}], "duration": 2419200, "startTimestamp": 1725494400, "strategiesAndMultipliers": [{"strategy": "0x5074dfd18e9498d9e006fb8d4f3fecdc9af90a2c", "multiplier": 1000000000000000000}, {"strategy": "0xD56e4eAb23cb81f43168F9F45211Eb027b9aC7cc", "multiplier": 2000000000000000000}], "description": "test reward submission"}}`,
			}

			err = model.SetupStateForBlock(blockNumber)
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
				{"0xD56e4eAb23cb81f43168F9F45211Eb027b9aC7cc", "2000000000000000000"},
			}

			operatorRewards := []struct {
				Operator string
				Amount   string
			}{
				{"0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1", "30000000000000000000000"},
				{"0xF50Cba7a66b5E615587157e43286DaA7aF94009e", "40000000000000000000000"},
			}

			typedChange := change.([]*OperatorDirectedOperatorSetRewardSubmission)
			assert.Equal(t, len(strategiesAndMultipliers)*len(operatorRewards), len(typedChange))

			for _, submission := range typedChange {
				assert.Equal(t, strings.ToLower("0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101"), strings.ToLower(submission.Avs))
				assert.Equal(t, uint64(1), submission.OperatorSetId)
				assert.Equal(t, strings.ToLower("0x0ddd9dc88e638aef6a8e42d0c98aaa6a48a98d24"), strings.ToLower(submission.Token))
				assert.Equal(t, strings.ToLower("0x7402669fb2c8a0cfe8108acb8a0070257c77ec6906ecb07d97c38e8a5ddc66a9"), strings.ToLower(submission.RewardHash))
				assert.Equal(t, uint64(2419200), submission.Duration)
				assert.Equal(t, int64(1725494400), submission.StartTimestamp.Unix())
				assert.Equal(t, int64(2419200+1725494400), submission.EndTimestamp.Unix())

				assert.Equal(t, strings.ToLower(strategiesAndMultipliers[submission.StrategyIndex].Strategy), strings.ToLower(submission.Strategy))
				assert.Equal(t, strategiesAndMultipliers[submission.StrategyIndex].Multiplier, submission.Multiplier)

				assert.Equal(t, strings.ToLower(operatorRewards[submission.OperatorIndex].Operator), strings.ToLower(submission.Operator))
				assert.Equal(t, operatorRewards[submission.OperatorIndex].Amount, submission.Amount)

				assert.Equal(t, "test reward submission", submission.Description)
			}

			err = model.CommitFinalState(blockNumber, false)
			assert.Nil(t, err)

			rewards := make([]*OperatorDirectedOperatorSetRewardSubmission, 0)
			query := `select * from operator_directed_operator_set_reward_submissions where block_number = ?`
			res := model.DB.Raw(query, blockNumber).Scan(&rewards)
			assert.Nil(t, res.Error)
			assert.Equal(t, len(strategiesAndMultipliers)*len(operatorRewards), len(rewards))

			submissionCounter += len(strategiesAndMultipliers) * len(operatorRewards)

			stateRoot, err := model.GenerateStateRoot(blockNumber)
			assert.Nil(t, err)
			assert.NotNil(t, stateRoot)
			assert.True(t, len(stateRoot) > 0)
		})
		t.Run("Ensure a submission with a duration of 0 is skipped", func(t *testing.T) {
			blockNumber := uint64(103)

			if err := createBlock(model, blockNumber); err != nil {
				t.Fatal(err)
			}

			log := &storage.TransactionLog{
				TransactionHash:  "some hash",
				TransactionIndex: big.NewInt(100).Uint64(),
				BlockNumber:      blockNumber,
				Address:          cfg.GetContractsMapForChain().RewardsCoordinator,
				Arguments:        `[{"Name": "caller", "Type": "address", "Value": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "Indexed": true}, {"Name": "operatorDirectedRewardsSubmissionHash", "Type": "bytes32", "Value": "0x7402669fb2c8a0cfe8108acb8a0070257c77ec6906ecb07d97c38e8a5ddc66a9", "Indexed": true}, {"Name": "operatorSet", "Type": "tuple", "Value": {"avs": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "id": 1}, "Indexed": false}, {"Name": "submissionNonce", "Type": "uint256", "Value": 0, "Indexed": false}, {"Name": "operatorDirectedRewardsSubmission", "Type": "((address,uint96)[],address,(address,uint256)[],uint32,uint32,string)", "Value": null, "Indexed": false}]`,
				EventName:        "OperatorDirectedOperatorSetRewardsSubmissionCreated",
				LogIndex:         big.NewInt(12).Uint64(),
				OutputData:       `{"operatorSet": {"avs": "0xd36b6e5eee8311d7bffb2f3bb33301a1ab7de101", "id": 1}, "submissionNonce": 0, "operatorDirectedRewardsSubmission": {"token": "0x0ddd9dc88e638aef6a8e42d0c98aaa6a48a98d24", "operatorRewards": [{"operator": "0x9401E5E6564DB35C0f86573a9828DF69Fc778aF1", "amount": 30000000000000000000000}, {"operator": "0xF50Cba7a66b5E615587157e43286DaA7aF94009e", "amount": 40000000000000000000000}], "duration": 0, "startTimestamp": 1725494400, "strategiesAndMultipliers": [{"strategy": "0x5074dfd18e9498d9e006fb8d4f3fecdc9af90a2c", "multiplier": 1000000000000000000}, {"strategy": "0xD56e4eAb23cb81f43168F9F45211Eb027b9aC7cc", "multiplier": 2000000000000000000}], "description": "test reward submission"}}`,
			}

			err = model.SetupStateForBlock(blockNumber)
			assert.Nil(t, err)

			isInteresting := model.IsInterestingLog(log)
			assert.True(t, isInteresting)

			change, err := model.HandleStateChange(log)
			assert.Nil(t, err)
			assert.NotNil(t, change)

			typedChange := change.([]*OperatorDirectedOperatorSetRewardSubmission)
			assert.Equal(t, 0, len(typedChange))
		})
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
