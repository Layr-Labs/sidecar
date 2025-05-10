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
	"github.com/Layr-Labs/sidecar/pkg/storage"
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
	chunks := chunkify(affectedBlocks, 1000)

	for _, chunk := range chunks {
		if err = s.handleChunk(ctx, chunk); err != nil {
			return fmt.Errorf("failed to handle chunk: %v", err)
		}
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
	numChunks := (len(blocks) / chunkSize) + 1
	var chunks [][]uint64
	for i := 0; i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(blocks) {
			end = len(blocks)
		}
		chunks = append(chunks, blocks[start:end])
	}
	return chunks
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

		replacedLog, err := helpers.WrapTxAndCommit(func(tx *gorm.DB) (*storage.TransactionLog, error) {
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
				return nil, res.Error
			}
			return s.indxr.IndexLog(ctx, log.BlockNumber.Value(), log.TransactionHash.Value(), log.TransactionIndex.Value(), decodedLog)
		}, s.grm, nil)
		if err != nil {
			return fmt.Errorf("failed to replace log: %v", err)
		}
		s.logger.Sugar().Debugw("Replaced log", zap.Any("log", replacedLog))
	}
	return nil
}
