package indexer

import (
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"go.uber.org/zap"
)

// ParseTransactionLogs processes a transaction and its receipt to extract and decode logs.
// It creates a ParsedTransaction containing decoded logs for events emitted by interesting contracts.
// Returns the parsed transaction with its logs, or nil if the transaction doesn't interact with any contracts.
// Returns an IndexError if contract retrieval, ABI parsing, or log decoding fails.
func (idx *Indexer) ParseTransactionLogs(
	transaction *ethereum.EthereumTransaction,
	receipt *ethereum.EthereumTransactionReceipt,
) (*parser.ParsedTransaction, error) {
	idx.Logger.Sugar().Debugw("ProcessingTransaction", zap.String("transactionHash", transaction.Hash.Value()))
	parsedTransaction := &parser.ParsedTransaction{
		Logs:        make([]*parser.DecodedLog, 0),
		Transaction: transaction,
		Receipt:     receipt,
	}

	logs, err := idx.TransactionLogParser.ParseTransactionLogs(receipt)
	if err != nil {
		return nil, err
	}

	parsedTransaction.Logs = logs
	return parsedTransaction, nil
}
