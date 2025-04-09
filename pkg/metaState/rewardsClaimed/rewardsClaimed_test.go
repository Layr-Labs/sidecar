package rewardsClaimed

import (
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/metaState/metaStateManager"
	"github.com/Layr-Labs/sidecar/pkg/metaState/types"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"os"
	"testing"
	"time"
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
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	dbname, _, grm, err := postgres.GetTestPostgresDatabase(cfg.DatabaseConfig, cfg, l)
	if err != nil {
		return dbname, nil, nil, nil, err
	}

	return dbname, grm, l, cfg, nil
}

func Test_RewardsClaimed(t *testing.T) {
	dbName, grm, l, cfg, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	msm := metaStateManager.NewMetaStateManager(grm, l, cfg)

	rewardsClaimedModel, err := NewRewardsClaimedModel(grm, l, cfg, msm)
	assert.Nil(t, err)

	t.Run("Should insert a rewardsClaimed event with a null recipient", func(t *testing.T) {
		block := &storage.Block{
			Number:    20535299,
			Hash:      "",
			BlockTime: time.Time{},
		}
		res := grm.Model(&storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		log := &storage.TransactionLog{
			TransactionHash:  "0x767e002f6f3a7942b22e38f2434ecd460fb2111b7ea584d16adb71692b856801",
			TransactionIndex: 77,
			Address:          "0x7750d328b314effa365a0402ccfd489b80b0adda",
			Arguments:        `[{"Name": "root", "Type": "bytes32", "Value": "0x0000000000000000000000003449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", "Indexed": false}, {"Name": "earner", "Type": "address", "Value": "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", "Indexed": true}, {"Name": "claimer", "Type": "address", "Value": "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", "Indexed": true}, {"Name": "recipient", "Type": "address", "Value": null, "Indexed": true}, {"Name": "token", "Type": "address", "Value": null, "Indexed": false}, {"Name": "claimedAmount", "Type": "uint256", "Value": null, "Indexed": false}]`,
			EventName:        "RewardsClaimed",
			OutputData:       `{"root": [200, 194, 94, 171, 12, 231, 185, 90, 53, 50, 87, 206, 179, 62, 194, 139, 92, 52, 159, 42, 165, 234, 249, 2, 180, 77, 155, 202, 81, 229, 100, 188], "token": "0x127500cd2030577f66d1b79600d30dcdba2ed32d", "claimedAmount": 306564275428435710000000}`,
			LogIndex:         270,
			BlockNumber:      block.Number,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err := rewardsClaimedModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		isInteresting := rewardsClaimedModel.IsInterestingLog(log)
		assert.True(t, isInteresting)

		state, err := rewardsClaimedModel.HandleTransactionLog(log)
		assert.Nil(t, err)

		typedState := state.(*types.RewardsClaimed)
		assert.Equal(t, "0xc8c25eab0ce7b95a353257ceb33ec28b5c349f2aa5eaf902b44d9bca51e564bc", typedState.Root)
		assert.Equal(t, "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", typedState.Earner)
		assert.Equal(t, "0x3449fe2810b0a5f6dffc62b8b6ee6b732dfe4438", typedState.Claimer)
		assert.Equal(t, "", typedState.Recipient)
		assert.Equal(t, "0x127500cd2030577f66d1b79600d30dcdba2ed32d", typedState.Token)
		assert.Equal(t, "306564275428435710000000", typedState.ClaimedAmount)
		assert.Equal(t, block.Number, typedState.BlockNumber)
		assert.Equal(t, log.TransactionHash, typedState.TransactionHash)
		assert.Equal(t, log.LogIndex, typedState.LogIndex)

		_, err = rewardsClaimedModel.CommitFinalState(block.Number)
		assert.Nil(t, err)

		// Check if the rewardsClaimed event was inserted
		var rewardsClaimed types.RewardsClaimed
		res = grm.Model(&types.RewardsClaimed{}).Where("block_number = ?", block.Number).First(&rewardsClaimed)
		assert.Nil(t, res.Error)

		err = rewardsClaimedModel.CleanupProcessedStateForBlock(block.Number)
		assert.Nil(t, err)

	})

	t.Run("Should insert a rewardsClaimed event with a not null", func(t *testing.T) {
		block := &storage.Block{
			Number:    20535362,
			Hash:      "",
			BlockTime: time.Time{},
		}
		res := grm.Model(&storage.Block{}).Create(&block)
		if res.Error != nil {
			t.Fatal(res.Error)
		}
		log := &storage.TransactionLog{
			TransactionHash:  "0x767e002f6f3a7942b22e38f2434ecd460fb2111b7ea584d16adb71692b856801",
			TransactionIndex: 42,
			Address:          "0x7750d328b314effa365a0402ccfd489b80b0adda",
			Arguments:        `[{"Name": "root", "Type": "bytes32", "Value": null, "Indexed": false}, {"Name": "earner", "Type": "address", "Value": "0x769e73da377876dd688b23d51ed01b7c7b154c65", "Indexed": true}, {"Name": "claimer", "Type": "address", "Value": "0x769e73da377876dd688b23d51ed01b7c7b154c65", "Indexed": true}, {"Name": "recipient", "Type": "address", "Value": "0x769e73da377876dd688b23d51ed01b7c7b154c65", "Indexed": true}, {"Name": "token", "Type": "address", "Value": null, "Indexed": false}, {"Name": "claimedAmount", "Type": "uint256", "Value": null, "Indexed": false}]`,
			EventName:        "RewardsClaimed",
			OutputData:       `{"root": [200, 194, 94, 171, 12, 231, 185, 90, 53, 50, 87, 206, 179, 62, 194, 139, 92, 52, 159, 42, 165, 234, 249, 2, 180, 77, 155, 202, 81, 229, 100, 188], "token": "0x127500cd2030577f66d1b79600d30dcdba2ed32d", "claimedAmount": 134162726422194540000000}`,
			LogIndex:         200,
			BlockNumber:      block.Number,
			CreatedAt:        time.Time{},
			UpdatedAt:        time.Time{},
			DeletedAt:        time.Time{},
		}

		err := rewardsClaimedModel.SetupStateForBlock(block.Number)
		assert.Nil(t, err)

		isInteresting := rewardsClaimedModel.IsInterestingLog(log)
		assert.True(t, isInteresting)

		state, err := rewardsClaimedModel.HandleTransactionLog(log)
		assert.Nil(t, err)

		typedState := state.(*types.RewardsClaimed)
		assert.Equal(t, "0xc8c25eab0ce7b95a353257ceb33ec28b5c349f2aa5eaf902b44d9bca51e564bc", typedState.Root)
		assert.Equal(t, "0x769e73da377876dd688b23d51ed01b7c7b154c65", typedState.Earner)
		assert.Equal(t, "0x769e73da377876dd688b23d51ed01b7c7b154c65", typedState.Claimer)
		assert.Equal(t, "0x769e73da377876dd688b23d51ed01b7c7b154c65", typedState.Recipient)
		assert.Equal(t, "0x127500cd2030577f66d1b79600d30dcdba2ed32d", typedState.Token)
		assert.Equal(t, "134162726422194540000000", typedState.ClaimedAmount)
		assert.Equal(t, block.Number, typedState.BlockNumber)
		assert.Equal(t, log.TransactionHash, typedState.TransactionHash)
		assert.Equal(t, log.LogIndex, typedState.LogIndex)

		_, err = rewardsClaimedModel.CommitFinalState(block.Number)
		assert.Nil(t, err)

		// Check if the rewardsClaimed event was inserted
		var rewardsClaimed types.RewardsClaimed
		res = grm.Model(&types.RewardsClaimed{}).Where("block_number = ?", block.Number).First(&rewardsClaimed)
		assert.Nil(t, res.Error)

		err = rewardsClaimedModel.CleanupProcessedStateForBlock(block.Number)
		assert.Nil(t, err)
	})

	t.Cleanup(func() {
		postgres.TeardownTestDatabase(dbName, cfg, grm, l)
	})
}
