package utils

import (
	"encoding/hex"
	"encoding/json"

	"github.com/Layr-Labs/go-sidecar/internal/parser"
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"go.uber.org/zap"
)

func ConvertBytesToString(b []byte) string {
	return "0x" + hex.EncodeToString(b)
}

func ParseLogArguments(logger *zap.Logger, log *storage.TransactionLog) ([]parser.Argument, error) {
	arguments := make([]parser.Argument, 0)
	err := json.Unmarshal([]byte(log.Arguments), &arguments)
	if err != nil {
		logger.Sugar().Errorw("Failed to unmarshal arguments",
			zap.Error(err),
			zap.String("transactionHash", log.TransactionHash),
			zap.Uint64("transactionIndex", log.TransactionIndex),
		)
		return nil, err
	}
	return arguments, nil
}

func ParseLogOutput(logger *zap.Logger, log *storage.TransactionLog) (map[string]interface{}, error) {
	outputData := make(map[string]interface{})
	err := json.Unmarshal([]byte(log.OutputData), &outputData)
	if err != nil {
		logger.Sugar().Errorw("Failed to unmarshal outputData",
			zap.Error(err),
			zap.String("transactionHash", log.TransactionHash),
			zap.Uint64("transactionIndex", log.TransactionIndex),
		)
		return nil, err
	}
	return outputData, nil
}
