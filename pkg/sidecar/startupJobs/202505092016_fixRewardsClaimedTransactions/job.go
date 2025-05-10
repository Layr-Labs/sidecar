package _202505092016_fixRewardsClaimedTransactions

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/fetcher"
	"github.com/Layr-Labs/sidecar/pkg/indexer"
	"github.com/Layr-Labs/sidecar/pkg/postgres/helpers"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"slices"
)

type StartupJob struct {
	ethClient *ethereum.Client
	indxr     *indexer.Indexer
	grm       *gorm.DB
	config    *config.Config
	fetcher   *fetcher.Fetcher
	logger    *zap.Logger
}

func (s *StartupJob) Run(
	ctx context.Context,
	cfg *config.Config,
	ethClient *ethereum.Client,
	indxr *indexer.Indexer,
	fetcher *fetcher.Fetcher,
	grm *gorm.DB,
	l *zap.Logger,
) error {
	s.Init(ctx, cfg, ethClient, indxr, fetcher, grm, l)

	// TODO(seanmcgary): if block number > some fork height and this job hasnt been run,
	// prune back to the fork then run this job and let the indexer catch up

	affectedBlocks, err := s.listRewardsClaimedBlocks()
	if err != nil {
		return err
	}

	// sort asc
	slices.Sort(affectedBlocks)

	// group blocks into chunks of 1000
	chunks := chunkify(affectedBlocks, 500)

	s.logger.Sugar().Infow("Chunked blocks",
		zap.Int("chunkCount", len(chunks)),
	)

	for i, chunk := range chunks {
		if err = s.handleChunk(ctx, chunk); err != nil {
			return fmt.Errorf("failed to handle chunk: %v", err)
		}
		s.logger.Sugar().Infow("Handled chunk",
			zap.Int("chunkIndex", i),
			zap.Int("chunkSize", len(chunk)),
		)
	}
	// rehydrate rewards claimed
	if err = s.rehydrateRewardsClaimed(); err != nil {
		return fmt.Errorf("failed to rehydrate rewards claimed: %v", err)
	}
	return nil
}

func (s *StartupJob) Init(
	ctx context.Context,
	cfg *config.Config,
	ethClient *ethereum.Client,
	indxr *indexer.Indexer,
	fetcher *fetcher.Fetcher,
	grm *gorm.DB,
	l *zap.Logger,
) {
	s.ethClient = ethClient
	s.indxr = indxr
	s.grm = grm
	s.config = cfg
	s.fetcher = fetcher
	s.logger = l
}

func (s *StartupJob) Name() string {
	return "202505092016_fixRewardsClaimedTransactions"
}

func (s *StartupJob) listRewardsClaimedBlocks() ([]uint64, error) {
	query := `
		select
			distinct(block_number)
		from transaction_logs
		where
			address = @rewardsCoordinatorAddress
			and event_name = 'RewardsClaimed'
		order by 1 asc
	`
	var blocks []uint64
	res := s.grm.Raw(query, sql.Named("rewardsCoordinatorAddress", s.config.GetContractsMapForChain().RewardsCoordinator)).Scan(&blocks)
	if res.Error != nil {
		return nil, res.Error
	}
	return blocks, nil
}
func chunkify(blocks []uint64, chunkSize int) [][]uint64 {
	// take the list of blocks and chunk it into chunks of size chunkSize.
	// if the distance between the current block and the previous block is greater than 200, start a new chunk.

	maxDiff := uint64(200)

	if len(blocks) == 0 {
		return [][]uint64{}
	}

	result := [][]uint64{}
	currentBatch := []uint64{blocks[0]}

	for i := 1; i < len(blocks); i++ {
		current := blocks[i]
		previous := blocks[i-1]

		// Start a new batch if:
		// 1. Current batch has reached max size (500), or
		// 2. Current number > previous number + 200
		if len(currentBatch) >= chunkSize || current > previous+maxDiff {
			result = append(result, currentBatch)
			currentBatch = []uint64{current}
		} else {
			currentBatch = append(currentBatch, current)
		}
	}

	// Don't forget to add the last batch
	if len(currentBatch) > 0 {
		result = append(result, currentBatch)
	}

	return result
}

func (s *StartupJob) handleChunk(ctx context.Context, blockNumbers []uint64) error {
	s.logger.Sugar().Infow("Handling chunk",
		zap.Int("chunkSize", len(blockNumbers)),
	)
	// get all blocks in the chunk
	mappedBlocks := map[uint64]*fetcher.FetchedBlock{}

	fetchedBlocks, err := s.fetcher.FetchBlockList(ctx, blockNumbers)
	if err != nil {
		return fmt.Errorf("failed to fetch blocks: %v", err)
	}
	if len(fetchedBlocks) == 0 {
		return fmt.Errorf("no blocks fetched")
	}
	if len(fetchedBlocks) != len(blockNumbers) {
		return fmt.Errorf("fetched blocks do not match requested blocks")
	}
	fmt.Printf("Fetched blocks: %+v\n", fetchedBlocks)

	for _, block := range fetchedBlocks {
		mappedBlocks[block.Number.Value()] = &fetcher.FetchedBlock{
			Block:      block,
			TxReceipts: map[string]*ethereum.EthereumTransactionReceipt{},
		}
	}

	minBlock := blockNumbers[0]
	maxBlock := blockNumbers[len(blockNumbers)-1]
	rewardsCoordinatorAddress := s.config.GetContractsMapForChain().RewardsCoordinator

	s.logger.Sugar().Infow("Fetching logs",
		zap.Uint64("minBlock", minBlock),
		zap.Uint64("maxBlock", maxBlock),
		zap.String("rewardsCoordinatorAddress", rewardsCoordinatorAddress),
	)

	// get logs for all blocks in the chunk by range
	logs, err := s.ethClient.GetLogs(ctx, rewardsCoordinatorAddress, minBlock, maxBlock)
	if err != nil {
		return fmt.Errorf("failed to get logs: %v", err)
	}
	fmt.Printf("Fetched logs: %+v\n", logs)

	for _, log := range logs {
		blockNumber := log.BlockNumber.Value()
		if _, ok := mappedBlocks[blockNumber]; !ok {
			continue
		}

		decodedLog, err := s.indxr.TransactionLogParser.DecodeLogWithAbi(nil, &ethereum.EthereumTransactionReceipt{
			To:              log.Address,
			ContractAddress: ethereum.EthereumHexString(""),
		}, log)
		if decodedLog == nil {
			continue
		}
		if decodedLog.EventName != "RewardsClaimed" {
			continue
		}
		if err != nil {
			return fmt.Errorf("failed to decode log: %v", err)
		}

		query := `
				delete from transaction_logs
				where
					transaction_hash = @transactionHash
					and log_index = @logIndex
					and block_number = @blockNumber
			`
		res := s.grm.Exec(query,
			sql.Named("transactionHash", log.TransactionHash),
			sql.Named("logIndex", log.LogIndex),
			sql.Named("blockNumber", blockNumber),
		)
		if res.Error != nil {
			return res.Error
		}
		replacedLog, err := s.indxr.IndexLog(ctx, log.BlockNumber.Value(), log.TransactionHash.Value(), log.TransactionIndex.Value(), decodedLog)
		if err != nil {
			return fmt.Errorf("failed to replace log: %v", err)
		}
		s.logger.Sugar().Debugw("Replaced log", zap.Any("log", replacedLog))
	}
	return nil
}

func (s *StartupJob) rehydrateRewardsClaimed() error {
	s.logger.Sugar().Infow("Rehydrating rewards claimed")
	_, err := helpers.WrapTxAndCommit(func(tx *gorm.DB) (interface{}, error) {

		truncateQuery := `truncate table rewards_claimed cascade`
		res := tx.Exec(truncateQuery)
		if res.Error != nil {
			return nil, res.Error
		}

		query := `
			insert into rewards_claimed (root, token, claimed_amount, earner, claimer, recipient, transaction_hash, block_number, log_index)
			select
				concat('0x', (
				  SELECT lower(string_agg(lpad(to_hex(elem::int), 2, '0'), ''))
				  FROM jsonb_array_elements_text(tl.output_data->'root') AS elem
				)) AS root,
				lower(tl.output_data->>'token'::text) as token,
				cast(tl.output_data->>'claimedAmount' as numeric) as claimed_amount,
				lower(tl.arguments #>> '{1, Value}') as earner,
				lower(tl.arguments #>> '{2, Value}') as claimer,
				lower(tl.arguments #>> '{3, Value}') as recipient,
				tl.transaction_hash,
				tl.block_number,
				tl.log_index
			from transaction_logs as tl
			where
				tl.address = @rewardsCoordinatorAddress
				and tl.event_name = 'RewardsClaimed'
			order by tl.block_number asc
			on conflict do nothing
		`
		contractAddresses := s.config.GetContractsMapForChain()
		res = tx.Exec(query, sql.Named("rewardsCoordinatorAddress", contractAddresses.RewardsCoordinator))
		return nil, res.Error
	}, s.grm, nil)
	return err
}
