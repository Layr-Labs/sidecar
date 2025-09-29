package baseModel

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"slices"
	"strings"
)

func IsInterestingLog(contractsEvents map[string][]string, log *storage.TransactionLog) bool {
	logAddress := strings.ToLower(log.Address)
	if eventNames, ok := contractsEvents[logAddress]; ok {
		if slices.Contains(eventNames, log.EventName) {
			return true
		}
	}
	return false
}

func ParseLogArguments(log *storage.TransactionLog, l *zap.Logger) ([]parser.Argument, error) {
	arguments := make([]parser.Argument, 0)
	err := json.Unmarshal([]byte(log.Arguments), &arguments)
	if err != nil {
		l.Sugar().Errorw("Failed to unmarshal arguments",
			zap.Error(err),
			zap.String("transactionHash", log.TransactionHash),
			zap.Uint64("transactionIndex", log.TransactionIndex),
		)
		return nil, err
	}
	return arguments, nil
}

func ParseLogOutput[T any](log *storage.TransactionLog, l *zap.Logger) (*T, error) {
	var outputData *T
	err := json.Unmarshal([]byte(log.OutputData), &outputData)
	if err != nil {
		l.Sugar().Errorw("Failed to unmarshal outputData",
			zap.Error(err),
			zap.String("transactionHash", log.TransactionHash),
			zap.Uint64("transactionIndex", log.TransactionIndex),
		)
		return nil, err
	}
	return outputData, nil
}

func DeleteState(tableName string, startBlockNumber uint64, endBlockNumber uint64, db *gorm.DB, l *zap.Logger) error {
	if endBlockNumber != 0 && endBlockNumber < startBlockNumber {
		l.Sugar().Errorw("Invalid block range",
			zap.Uint64("startBlockNumber", startBlockNumber),
			zap.Uint64("endBlockNumber", endBlockNumber),
		)
		return errors.New("Invalid block range; endBlockNumber must be greater than or equal to startBlockNumber")
	}

	// tokenizing the table name apparently doesnt work, so we need to use Sprintf to include it.
	query := fmt.Sprintf(`
		delete from %s
		where block_number >= @startBlockNumber
	`, tableName)
	if endBlockNumber > 0 {
		query += " and block_number <= @endBlockNumber"
	}
	res := db.Exec(query,
		sql.Named("tableName", tableName),
		sql.Named("startBlockNumber", startBlockNumber),
		sql.Named("endBlockNumber", endBlockNumber))
	if res.Error != nil {
		l.Sugar().Errorw("Failed to delete state", zap.Error(res.Error))
		return res.Error
	}
	return nil
}

func CastCommittedStateToInterface[T any](committedState []*T) []interface{} {
	state := make([]interface{}, len(committedState))
	for i, v := range committedState {
		state[i] = v
	}
	return state
}
