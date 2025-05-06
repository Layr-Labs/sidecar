package postgres

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/Layr-Labs/sidecar/pkg/storage"

	"github.com/Layr-Labs/sidecar/internal/config"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type PostgresBlockStore struct {
	Db *gorm.DB
	//nolint:unused
	migrated     bool
	Logger       *zap.Logger
	GlobalConfig *config.Config
}

func NewPostgresBlockStore(db *gorm.DB, l *zap.Logger, cfg *config.Config) *PostgresBlockStore {
	bs := &PostgresBlockStore{
		Db:           db,
		Logger:       l,
		GlobalConfig: cfg,
	}
	return bs
}

func (s *PostgresBlockStore) InsertBlockAtHeight(
	blockNumber uint64,
	hash string,
	parentHash string,
	blockTime uint64,
) (*storage.Block, error) {
	block := &storage.Block{
		Number:     blockNumber,
		Hash:       hash,
		ParentHash: parentHash,
		BlockTime:  time.Unix(int64(blockTime), 0),
	}

	res := s.Db.Model(&storage.Block{}).Clauses(clause.Returning{}).Create(&block)

	if res.Error != nil {
		return nil, fmt.Errorf("failed to insert block with number '%d': %w", blockNumber, res.Error)
	}
	return block, nil
}

func (s *PostgresBlockStore) InsertBlockTransaction(
	blockNumber uint64,
	txHash string,
	txIndex uint64,
	from string,
	to string,
	contractAddress string,
	bytecodeHash string,
	gasUsed uint64,
	cumulativeGasUsed uint64,
	effectiveGasPrice uint64,
) (*storage.Transaction, error) {
	to = strings.ToLower(to)
	from = strings.ToLower(from)
	contractAddress = strings.ToLower(contractAddress)

	tx := &storage.Transaction{
		BlockNumber:       blockNumber,
		TransactionHash:   txHash,
		TransactionIndex:  txIndex,
		FromAddress:       from,
		ToAddress:         to,
		ContractAddress:   contractAddress,
		BytecodeHash:      bytecodeHash,
		GasUsed:           gasUsed,
		CumulativeGasUsed: cumulativeGasUsed,
		EffectiveGasPrice: effectiveGasPrice,
	}

	result := s.Db.Model(&storage.Transaction{}).Clauses(clause.Returning{}).Create(&tx)

	if result.Error != nil {
		return nil, fmt.Errorf("Failed to insert block transaction '%d' - '%s': %w", blockNumber, txHash, result.Error)
	}
	return tx, nil
}

func (s *PostgresBlockStore) InsertTransactionLog(
	txHash string,
	transactionIndex uint64,
	blockNumber uint64,
	log *parser.DecodedLog,
	outputData map[string]interface{},
	ignoreOnConflict bool,
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
		Address:          strings.ToLower(log.Address),
		Arguments:        string(argsJson),
		EventName:        log.EventName,
		LogIndex:         log.LogIndex,
		OutputData:       string(outputDataJson),
	}
	clauses := []clause.Expression{clause.Returning{}}
	if ignoreOnConflict {
		clauses = append(clauses, clause.OnConflict{
			Columns:   []clause.Column{{Name: "transaction_hash"}, {Name: "log_index"}},
			DoNothing: true,
		})
	}
	result := s.Db.Model(&storage.TransactionLog{}).Clauses(clauses...).Create(&txLog)

	if result.Error != nil {
		return nil, fmt.Errorf("Failed to insert transaction log: %w - %+v", result.Error, txLog)
	}
	return txLog, nil
}

func (s *PostgresBlockStore) GetLatestBlock() (*storage.Block, error) {
	block := &storage.Block{}

	query := `
	select
	 *
	from blocks
	order by number desc
	limit 1`

	result := s.Db.Model(&storage.Block{}).Raw(query).Scan(&block)
	if result.Error != nil {
		return nil, fmt.Errorf("Failed to get latest block: %w", result.Error)
	}
	return block, nil
}

func (s *PostgresBlockStore) GetBlockByNumber(blockNumber uint64) (*storage.Block, error) {
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

func (s *PostgresBlockStore) InsertOperatorRestakedStrategies(
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
		return nil, fmt.Errorf("Failed to insert operator restaked strategies: %w", result.Error)
	}
	return ors, nil
}

func (s *PostgresBlockStore) BulkInsertOperatorRestakedStrategies(
	operatorRestakedStrategies []*storage.OperatorRestakedStrategies,
) ([]*storage.OperatorRestakedStrategies, error) {
	if len(operatorRestakedStrategies) == 0 {
		return operatorRestakedStrategies, nil
	}
	res := s.Db.Model(&storage.OperatorRestakedStrategies{}).Clauses(
		clause.OnConflict{
			OnConstraint: "uniq_operator_restaked_strategies",
			DoNothing:    true,
		},
	).CreateInBatches(&operatorRestakedStrategies, 5000)
	if res.Error != nil {
		return nil, fmt.Errorf("Failed to insert operator restaked strategies: %w", res.Error)
	}
	return operatorRestakedStrategies, nil
}

func (s *PostgresBlockStore) GetLatestActiveAvsOperators(blockNumber uint64, avsDirectoryAddress string) ([]*storage.ActiveAvsOperator, error) {
	avsDirectoryAddress = strings.ToLower(avsDirectoryAddress)

	rows := make([]*storage.ActiveAvsOperator, 0)
	query := `
		WITH latest_status AS (
			SELECT 
				lower(tl.arguments #>> '{0,Value}') as operator,
				lower(tl.arguments #>> '{1,Value}') as avs,
				lower(tl.output_data #>> '{status}') as status,
				ROW_NUMBER() OVER (PARTITION BY lower(tl.arguments #>> '{0,Value}'), lower(tl.arguments #>> '{1,Value}') ORDER BY block_number DESC, log_index desc) AS row_number
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
		return nil, fmt.Errorf("Failed to get latest active AVS operators: %w", result.Error)
	}
	return rows, nil
}

func (s *PostgresBlockStore) DeleteCorruptedState(startBlockNumber uint64, endBlockNumber uint64) error {
	s.Logger.Sugar().Infow("Deleting corrupted state",
		zap.Uint64("startBlockNumber", startBlockNumber),
		zap.Uint64("endBlockNumber", endBlockNumber),
	)
	if endBlockNumber != 0 && endBlockNumber < startBlockNumber {
		s.Logger.Sugar().Errorw("Invalid block range",
			zap.Uint64("startBlockNumber", startBlockNumber),
			zap.Uint64("endBlockNumber", endBlockNumber),
		)
		return fmt.Errorf("Invalid block range; endBlockNumber must be greater than or equal to startBlockNumber")
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
			return fmt.Errorf("Failed to delete corrupted state from table '%s': %w", tableName, res.Error)
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
		return fmt.Errorf("Failed to delete corrupted state from table 'blocks': %w", res.Error)
	}
	return nil
}
