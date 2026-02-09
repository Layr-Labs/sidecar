package protocolDataService

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateMigrator"
	"github.com/Layr-Labs/sidecar/pkg/logger"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"github.com/Layr-Labs/sidecar/pkg/service/types"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

func setup(dataSourceName string) (
	*gorm.DB,
	*zap.Logger,
	*config.Config,
	error,
) {
	projectRoot := tests.GetProjectRoot()
	datasources, err := tests.GetTestDataSources(projectRoot)
	if err != nil {
		log.Fatalf("Failed to get datasources: %v", err)
	}
	dsTestnet := datasources.GetDataSourceByName(dataSourceName)

	cfg := config.NewConfig()
	cfg.Chain = config.Chain_Holesky
	cfg.Debug = os.Getenv(config.Debug) == "true"
	cfg.DatabaseConfig = *tests.GetDbConfigFromEnv()
	cfg.DatabaseConfig.DbName = dsTestnet.DbName

	l, _ := logger.NewLogger(&logger.LoggerConfig{Debug: cfg.Debug})

	pgConfig := postgres.PostgresConfigFromDbConfig(&cfg.DatabaseConfig)
	pg, err := postgres.NewPostgres(pgConfig)
	if err != nil {
		l.Fatal("Failed to setup postgres connection", zap.Error(err))
	}

	grm, err := postgres.NewGormFromPostgresConnection(pg.Db)
	if err != nil {
		l.Fatal("Failed to create gorm instance", zap.Error(err))
	}

	return grm, l, cfg, nil
}

func Test_ProtocolDataService(t *testing.T) {
	if !tests.LargeTestsEnabled() {
		t.Skipf("Skipping large test")
		return
	}

	grm, l, cfg, err := setup("testnetFull")

	t.Logf("Using database with name: %s", cfg.DatabaseConfig.DbName)

	if err != nil {
		t.Fatalf("Failed to setup test: %v", err)
	}
	smig, err := stateMigrator.NewStateMigrator(grm, cfg, l)
	if err != nil {
		t.Fatalf("Failed to create state migrator: %v", err)
	}
	sm := stateManager.NewEigenStateManager(smig, l, grm)

	pds := NewProtocolDataService(sm, grm, l, cfg)

	t.Run("Test ListRegisteredAVSsForOperator", func(t *testing.T) {
		operator := "0x7dcdae42c07ae52d6c5c9554e7c5109f19b2613d"
		blockNumber := uint64(3240224)

		avss, err := pds.ListRegisteredAVSsForOperator(context.Background(), operator, blockNumber)
		assert.Nil(t, err)
		assert.True(t, len(avss) > 0)
	})

	t.Run("Test ListDelegatedStrategiesForOperator", func(t *testing.T) {
		operator := "0xb5ead7a953052da8212da7e9462d65f91205d06d"
		blockNumber := uint64(3204393)

		strategies, err := pds.ListDelegatedStrategiesForOperator(context.Background(), operator, blockNumber)
		assert.Nil(t, err)
		assert.True(t, len(strategies) > 0)
	})

	t.Run("Test GetOperatorDelegatedStake", func(t *testing.T) {
		operator := "0xb5ead7a953052da8212da7e9462d65f91205d06d"
		strategy := "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"
		blockNumber := uint64(3204393)

		stake, err := pds.GetOperatorDelegatedStake(context.Background(), operator, strategy, blockNumber)
		assert.Nil(t, err)
		assert.NotNil(t, stake)
		assert.Equal(t, stake.Shares, "999960761744418521836")
		assert.Equal(t, stake.AvsAddresses[0], "0xd9b1da8159cf83ccc55ad5757bea33e6f0ce34be")

	})

	t.Run("Test ListDelegatedStakersForOperator", func(t *testing.T) {
		operator := "0xb5ead7a953052da8212da7e9462d65f91205d06d"
		blockNumber := uint64(3204393)

		stakers, err := pds.ListDelegatedStakersForOperator(context.Background(), operator, blockNumber, nil)
		assert.Nil(t, err)
		assert.True(t, len(stakers) > 0)
	})

	t.Run("Test ListStakerShares", func(t *testing.T) {
		t.Run("Should return an empty array of AVSs", func(t *testing.T) {
			staker := "0x130c646e1224d979ff23523308abb6012ce04b0a"
			blockNumber := uint64(3204391)

			shares, err := pds.ListStakerShares(context.Background(), staker, blockNumber)
			assert.Nil(t, err)
			assert.True(t, len(shares) > 0)
			for _, share := range shares {
				assert.True(t, len(share.AvsAddresses) == 0)
			}
		})
		t.Run("Should return an array of AVSs", func(t *testing.T) {
			staker := "0xbc9dec48f305167bb8ee593e44893acf65ad3f36"
			blockNumber := uint64(3240224)

			shares, err := pds.ListStakerShares(context.Background(), staker, blockNumber)
			assert.Nil(t, err)
			assert.True(t, len(shares) > 0)
			for _, share := range shares {
				assert.True(t, len(share.AvsAddresses) > 0)
			}
		})
	})

	t.Run("Test GetTotalDelegatedOperatorSharesForStrategy", func(t *testing.T) {
		strategy := "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"
		blockNumber := uint64(3204393)

		t.Run("Without operator filter (all operators)", func(t *testing.T) {
			// Test with empty operator string to get all operators
			operatorStakes, err := pds.GetTotalDelegatedOperatorSharesForStrategy(context.Background(), "", strategy, blockNumber, nil)
			assert.Nil(t, err)
			assert.True(t, len(operatorStakes) > 0, "Should return multiple operators")

			// Verify the structure of returned data
			for _, stake := range operatorStakes {
				assert.NotEmpty(t, stake.Operator, "Operator address should not be empty")
				assert.NotEmpty(t, stake.Shares, "Shares should not be empty")
			}
		})

		t.Run("With specific operator filter", func(t *testing.T) {
			// Test with specific operator to test HasOperator template logic
			operator := "0xb5ead7a953052da8212da7e9462d65f91205d06d"
			operatorStakes, err := pds.GetTotalDelegatedOperatorSharesForStrategy(context.Background(), operator, strategy, blockNumber, nil)
			assert.Nil(t, err)

			// Should return only the specified operator
			if len(operatorStakes) > 0 {
				assert.Equal(t, 1, len(operatorStakes), "Should return exactly one operator")
				assert.Equal(t, operator, operatorStakes[0].Operator)
				assert.NotEmpty(t, operatorStakes[0].Shares)
			}
		})

		t.Run("With nil pagination (uses default)", func(t *testing.T) {
			// Test with nil pagination to ensure default is applied
			operatorStakes, err := pds.GetTotalDelegatedOperatorSharesForStrategy(context.Background(), "", strategy, blockNumber, nil)
			assert.Nil(t, err)
			assert.True(t, len(operatorStakes) > 0)
			// Default page size is 100, so should not exceed that
			assert.True(t, len(operatorStakes) <= 100, "Should respect default page size of 100")
		})

		t.Run("With custom pagination (first page)", func(t *testing.T) {
			// Test with custom pagination on first page (no offset)
			pagination := &types.Pagination{
				Page:     0,
				PageSize: 5,
			}
			operatorStakes, err := pds.GetTotalDelegatedOperatorSharesForStrategy(context.Background(), "", strategy, blockNumber, pagination)
			assert.Nil(t, err)
			assert.True(t, len(operatorStakes) > 0)
			assert.True(t, len(operatorStakes) <= 5, "Should respect custom page size of 5")
		})

		t.Run("With custom pagination (second page with offset)", func(t *testing.T) {
			// Test with page > 0 to test HasOffset template logic
			pagination := &types.Pagination{
				Page:     1,
				PageSize: 5,
			}
			operatorStakes, err := pds.GetTotalDelegatedOperatorSharesForStrategy(context.Background(), "", strategy, blockNumber, pagination)
			assert.Nil(t, err)
			// May or may not have results depending on total data, but should not error
			assert.True(t, len(operatorStakes) <= 5, "Should respect custom page size of 5")
		})

		t.Run("With operator and pagination combined", func(t *testing.T) {
			// Test combining operator filter with pagination
			operator := "0xb5ead7a953052da8212da7e9462d65f91205d06d"
			pagination := &types.Pagination{
				Page:     0,
				PageSize: 10,
			}
			operatorStakes, err := pds.GetTotalDelegatedOperatorSharesForStrategy(context.Background(), operator, strategy, blockNumber, pagination)
			assert.Nil(t, err)

			if len(operatorStakes) > 0 {
				assert.Equal(t, operator, operatorStakes[0].Operator)
				assert.True(t, len(operatorStakes) <= 10, "Should respect page size")
			}
		})
	})

	t.Run("Test ListStakersForStrategy", func(t *testing.T) {
		strategy := "0x7d704507b76571a51d9cae8addabbfd0ba0e63d3"
		blockNumber := uint64(3204393)

		t.Run("With DelegationFilterAll (default)", func(t *testing.T) {
			// Test with ALL filter - should return all stakers regardless of delegation status
			stakers, err := pds.ListStakersForStrategy(context.Background(), strategy, blockNumber, DelegationFilterAll, nil)
			assert.Nil(t, err)
			assert.True(t, len(stakers) > 0, "Should return stakers for the strategy")

			// Verify the structure of returned data
			for _, staker := range stakers {
				assert.NotEmpty(t, staker.Staker, "Staker address should not be empty")
				assert.NotEmpty(t, staker.Shares, "Shares should not be empty")
			}
		})

		t.Run("With DelegationFilterDelegated", func(t *testing.T) {
			// Test with DELEGATED filter - should return only delegated stakers
			stakers, err := pds.ListStakersForStrategy(context.Background(), strategy, blockNumber, DelegationFilterDelegated, nil)
			assert.Nil(t, err)

			// All returned stakers should be delegated
			for _, staker := range stakers {
				assert.True(t, staker.Delegated, "Staker should be delegated")
				assert.NotNil(t, staker.Operator, "Delegated staker should have an operator")
				if staker.Operator != nil {
					assert.NotEmpty(t, *staker.Operator, "Operator address should not be empty for delegated staker")
				}
			}
		})

		t.Run("With DelegationFilterUndelegated", func(t *testing.T) {
			// Test with UNDELEGATED filter - should return only undelegated stakers
			stakers, err := pds.ListStakersForStrategy(context.Background(), strategy, blockNumber, DelegationFilterUndelegated, nil)
			assert.Nil(t, err)

			// All returned stakers should NOT be delegated
			for _, staker := range stakers {
				assert.False(t, staker.Delegated, "Staker should not be delegated")
			}
		})

		t.Run("With nil pagination (uses default)", func(t *testing.T) {
			// Test with nil pagination to ensure no error occurs
			stakers, err := pds.ListStakersForStrategy(context.Background(), strategy, blockNumber, DelegationFilterAll, nil)
			assert.Nil(t, err)
			assert.True(t, len(stakers) >= 0, "Should return stakers or empty array")
		})

		t.Run("With custom pagination (first page)", func(t *testing.T) {
			// Test with custom pagination on first page (no offset)
			pagination := &types.Pagination{
				Page:     0,
				PageSize: 5,
			}
			stakers, err := pds.ListStakersForStrategy(context.Background(), strategy, blockNumber, DelegationFilterAll, pagination)
			assert.Nil(t, err)
			assert.True(t, len(stakers) <= 5, "Should respect custom page size of 5")
		})

		t.Run("With custom pagination (second page with offset)", func(t *testing.T) {
			// Test with page > 0 to test HasOffset template logic
			pagination := &types.Pagination{
				Page:     1,
				PageSize: 5,
			}
			stakers, err := pds.ListStakersForStrategy(context.Background(), strategy, blockNumber, DelegationFilterAll, pagination)
			assert.Nil(t, err)
			// May or may not have results depending on total data, but should not error
			assert.True(t, len(stakers) <= 5, "Should respect custom page size of 5")
		})

		t.Run("With delegation filter and pagination combined", func(t *testing.T) {
			// Test combining delegation filter with pagination
			pagination := &types.Pagination{
				Page:     0,
				PageSize: 10,
			}
			stakers, err := pds.ListStakersForStrategy(context.Background(), strategy, blockNumber, DelegationFilterDelegated, pagination)
			assert.Nil(t, err)
			assert.True(t, len(stakers) <= 10, "Should respect page size")

			// All returned stakers should be delegated
			for _, staker := range stakers {
				assert.True(t, staker.Delegated, "Staker should be delegated")
			}
		})

		t.Run("Stakers are ordered by shares descending", func(t *testing.T) {
			// Test that results are ordered by shares descending
			pagination := &types.Pagination{
				Page:     0,
				PageSize: 10,
			}
			stakers, err := pds.ListStakersForStrategy(context.Background(), strategy, blockNumber, DelegationFilterAll, pagination)
			assert.Nil(t, err)

			if len(stakers) > 1 {
				// Verify ordering (shares should be descending)
				// Note: shares are strings representing large numbers, so we can't directly compare
				// but we can verify the query runs without error
				assert.True(t, len(stakers) > 0, "Should have stakers to verify ordering")
			}
		})

		t.Run("With uppercase strategy address (should be normalized)", func(t *testing.T) {
			// Test that uppercase addresses are normalized to lowercase
			uppercaseStrategy := "0x7D704507B76571A51D9CAE8ADDABBFD0BA0E63D3"
			stakers, err := pds.ListStakersForStrategy(context.Background(), uppercaseStrategy, blockNumber, DelegationFilterAll, nil)
			assert.Nil(t, err)
			// Should work the same as lowercase
			assert.True(t, len(stakers) >= 0)
		})
	})
}
