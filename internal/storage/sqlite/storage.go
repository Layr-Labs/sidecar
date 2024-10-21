package sqlite

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Layr-Labs/go-sidecar/internal/config"
	"github.com/Layr-Labs/go-sidecar/internal/parser"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SqliteBlockStoreConfig struct {
	DbLocation string
}

type SqliteBlockStore struct {
	Db *gorm.DB
	//nolint:unused
	migrated     bool
	Logger       *zap.Logger
	GlobalConfig *config.Config
}

func NewSqliteBlockStore(db *gorm.DB, l *zap.Logger, cfg *config.Config) *SqliteBlockStore {
	bs := &SqliteBlockStore{
		Db:           db,
		Logger:       l,
		GlobalConfig: cfg,
	}
	return bs
}

func (s *SqliteBlockStore) InsertBlockAtHeight(
	blockNumber uint64,
	hash string,
	blockTime uint64,
) (*storage.Block, error) {
	block := &storage.Block{
		Number:    blockNumber,
		Hash:      hash,
		BlockTime: time.Unix(int64(blockTime), 0),
	}

	res := s.Db.Model(&storage.Block{}).Clauses(clause.Returning{}).Create(&block)

	if res.Error != nil {
		return nil, fmt.Errorf("failed to insert block with number '%d': %w", blockNumber, res.Error)
	}
	return block, nil
}

func (s *SqliteBlockStore) InsertBlockTransaction(
	blockNumber uint64,
	blockTime time.Time,
	txHash string,
	txIndex uint64,
	from string,
	to string,
	contractAddress string,
	bytecodeHash string,
) (*storage.Transaction, error) {
	to = strings.ToLower(to)
	from = strings.ToLower(from)
	contractAddress = strings.ToLower(contractAddress)

	tx := &storage.Transaction{
		BlockNumber:      blockNumber,
		BlockTime:        blockTime,
		TransactionHash:  txHash,
		TransactionIndex: txIndex,
		FromAddress:      from,
		ToAddress:        to,
		ContractAddress:  contractAddress,
		BytecodeHash:     bytecodeHash,
	}

	result := s.Db.Model(&storage.Transaction{}).Clauses(clause.Returning{}).Create(&tx)

	if result.Error != nil {
		return nil, xerrors.Errorf("Failed to insert block transaction '%d' - '%s': %w", blockNumber, txHash, result.Error)
	}
	return tx, nil
}

func (s *SqliteBlockStore) InsertTransactionLog(
	txHash string,
	transactionIndex uint64,
	blockNumber uint64,
	blockTime time.Time,
	log *parser.DecodedLog,
	outputData map[string]interface{},
) (*storage.TransactionLog, error) {
	argsJson, err := json.Marshal(log.Arguments)
	if err != nil {
		s.Logger.Sugar().Errorw("Failed to marshal arguments", zap.Error(err))
	}

	outputDataJson, err := json.Marshal(outputData)
	if err != nil {
		s.Logger.Sugar().Errorw("Failed to marshal output data", zap.Error(err))
	}

	txLog := &storage.TransactionLog{
		TransactionHash:  txHash,
		TransactionIndex: transactionIndex,
		BlockNumber:      blockNumber,
		BlockTime:        blockTime,
		Address:          strings.ToLower(log.Address),
		Arguments:        string(argsJson),
		EventName:        log.EventName,
		LogIndex:         log.LogIndex,
		OutputData:       string(outputDataJson),
	}
	result := s.Db.Model(&storage.TransactionLog{}).Clauses(clause.Returning{}).Create(&txLog)

	if result.Error != nil {
		return nil, xerrors.Errorf("Failed to insert transaction log: %w - %+v", result.Error, txLog)
	}
	return txLog, nil
}

func (s *SqliteBlockStore) GetLatestBlock() (*storage.Block, error) {
	block := &storage.Block{}

	query := `
	select
	 *
	from blocks
	order by number desc
	limit 1`

	result := s.Db.Model(&storage.Block{}).Raw(query).Scan(&block)
	if result.Error != nil {
		return nil, xerrors.Errorf("Failed to get latest block: %w", result.Error)
	}
	return block, nil
}

func (s *SqliteBlockStore) GetBlockByNumber(blockNumber uint64) (*storage.Block, error) {
	block := &storage.Block{}

	result := s.Db.Model(block).Where("number = ?", blockNumber).First(&block)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, result.Error
	}
	return block, nil
}

func (s *SqliteBlockStore) InsertOperatorRestakedStrategies(
	avsDirectorAddress string,
	blockNumber uint64,
	blockTime time.Time,
	operator string,
	avs string,
	strategy string,
) (*storage.OperatorRestakedStrategies, error) {
	ors := &storage.OperatorRestakedStrategies{
		AvsDirectoryAddress: strings.ToLower(avsDirectorAddress),
		BlockNumber:         blockNumber,
		Operator:            operator,
		Avs:                 avs,
		Strategy:            strategy,
		BlockTime:           blockTime,
	}

	result := s.Db.Model(&storage.OperatorRestakedStrategies{}).Clauses(clause.Returning{}).Create(&ors)

	if result.Error != nil {
		return nil, xerrors.Errorf("Failed to insert operator restaked strategies: %w", result.Error)
	}
	return ors, nil
}

func (s *SqliteBlockStore) GetLatestActiveAvsOperators(blockNumber uint64, avsDirectoryAddress string) ([]*storage.ActiveAvsOperator, error) {
	avsDirectoryAddress = strings.ToLower(avsDirectoryAddress)

	rows := make([]*storage.ActiveAvsOperator, 0)
	query := `
		WITH latest_status AS (
			SELECT
				lower(json_extract(tl.arguments, '$[0].Value')) as operator,
				lower(json_extract(tl.arguments, '$[1].Value')) as avs,
				lower(json_extract(tl.output_data, '$.status')) as status,
				ROW_NUMBER() OVER (
					PARTITION BY
						lower(json_extract(tl.arguments, '$[0].Value')),
						lower(json_extract(tl.arguments, '$[1].Value'))
					ORDER BY block_number DESC
					) AS row_number
			FROM transaction_logs as tl
			WHERE
				tl.address = ?
				AND tl.event_name = 'OperatorAVSRegistrationStatusUpdated'
				AND tl.block_number <= ?
		)
		SELECT avs, operator
		FROM latest_status
		WHERE row_number = 1 AND status = '1';
	`
	result := s.Db.Raw(query, avsDirectoryAddress, blockNumber).Scan(&rows)
	if result.Error != nil {
		return nil, xerrors.Errorf("Failed to get latest active AVS operators: %w", result.Error)
	}
	return rows, nil
}

func (s *SqliteBlockStore) DeleteCorruptedState(startBlockNumber uint64, endBlockNumber uint64) error {
	if endBlockNumber != 0 && endBlockNumber < startBlockNumber {
		s.Logger.Sugar().Errorw("Invalid block range",
			zap.Uint64("startBlockNumber", startBlockNumber),
			zap.Uint64("endBlockNumber", endBlockNumber),
		)
		return fmt.Errorf("invalid block range; endBlockNumber must be greater than or equal to startBlockNumber")
	}

	tablesWithBlockNumber := []string{
		"transaction_logs",
		"transactions",
	}

	for _, tableName := range tablesWithBlockNumber {
		query := fmt.Sprintf(`
			delete from %s
			where block_number >= @startBlockNumber
		`, tableName)
		if endBlockNumber > 0 {
			query += " and block_number <= @endBlockNumber"
		}
		res := s.Db.Exec(query,
			sql.Named("startBlockNumber", startBlockNumber),
			sql.Named("endBlockNumber", endBlockNumber),
		)
		if res.Error != nil {
			return xerrors.Errorf("Failed to delete corrupted state from table '%s': %w", tableName, res.Error)
		}
	}
	blocksQuery := `
		delete from blocks
		where number >= @startBlockNumber
	`
	if endBlockNumber > 0 {
		blocksQuery += " and number <= @endBlockNumber"
	}
	res := s.Db.Exec(blocksQuery,
		sql.Named("startBlockNumber", startBlockNumber),
		sql.Named("endBlockNumber", endBlockNumber),
	)
	if res.Error != nil {
		return xerrors.Errorf("Failed to delete corrupted state from table 'blocks': %w", res.Error)
	}
	return nil
}
