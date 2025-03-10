package indexer

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"go.uber.org/zap"
	"regexp"
)

// getAbi unmarshals a JSON ABI string into an abi.ABI struct.
// It handles certain common unmarshaling errors that can be safely ignored,
// such as "only single receive is allowed" and "only single fallback is allowed".
// Returns the parsed ABI and any error encountered during parsing.
func (idx *Indexer) getAbi(json string) (*abi.ABI, error) {
	a := &abi.ABI{}

	err := a.UnmarshalJSON([]byte(json))

	if err != nil {
		foundMatch := false
		// patterns that we're fine to ignore and not treat as an error
		patterns := []*regexp.Regexp{
			regexp.MustCompile(`only single receive is allowed`),
			regexp.MustCompile(`only single fallback is allowed`),
		}

		for _, pattern := range patterns {
			if pattern.MatchString(err.Error()) {
				foundMatch = true
				break
			}
		}

		// If the error isnt one that we can ignore, return it
		if !foundMatch {
			idx.Logger.Sugar().Warnw("Error unmarshaling abi json", zap.Error(err))
			return nil, err
		}
	}

	return a, nil
}

// ParseTransactionLogs processes a transaction and its receipt to extract and decode logs.
// It creates a ParsedTransaction containing decoded logs for events emitted by interesting contracts.
// Returns the parsed transaction with its logs, or nil if the transaction doesn't interact with any contracts.
// Returns an IndexError if contract retrieval, ABI parsing, or log decoding fails.
func (idx *Indexer) ParseTransactionLogs(
	transaction *ethereum.EthereumTransaction,
	receipt *ethereum.EthereumTransactionReceipt,
) (*parser.ParsedTransaction, *IndexError) {
	idx.Logger.Sugar().Debugw("ProcessingTransaction", zap.String("transactionHash", transaction.Hash.Value()))
	parsedTransaction := &parser.ParsedTransaction{
		Logs:        make([]*parser.DecodedLog, 0),
		Transaction: transaction,
		Receipt:     receipt,
	}

	contractAddress := receipt.GetTargetAddress()
	// when the contractAddress is empty, we can skip the transaction since it wont have any logs associated with it.
	// this is pretty typical of a transaction that simply sends ETH from one address to another without interacting with a contract
	if contractAddress.Value() == "" {
		idx.Logger.Sugar().Debugw("No contract address found in receipt, skipping", zap.String("hash", transaction.Hash.Value()))
		return nil, nil
	}

	// Check if the transaction address is interesting.
	// It may be the case that the transaction isnt interesting, but it emitted an interesting log, in which case
	// the log address would be different than the transaction address
	var a *abi.ABI
	if idx.IsInterestingAddress(contractAddress.Value()) {
		contract, err := idx.ContractManager.GetContractWithProxy(contractAddress.Value(), transaction.BlockNumber.Value())
		if err != nil {
			idx.Logger.Sugar().Errorw(fmt.Sprintf("Failed to get contract for address %s", contractAddress), zap.Error(err))
			return nil, NewIndexError(IndexError_FailedToFindContract, err).
				WithMessage("Failed to find contract").
				WithBlockNumber(transaction.BlockNumber.Value()).
				WithTransactionHash(transaction.Hash.Value()).
				WithMetadata("contractAddress", contractAddress.Value())
		}

		// if the contract is interesting but not found, throw an error to stop processing
		if contract == nil {
			idx.Logger.Sugar().Errorw("No contract found for address", zap.String("hash", transaction.Hash.Value()))
			return nil, NewIndexError(IndexError_FailedToFindContract, err).
				WithMessage("No contract found for address").
				WithBlockNumber(transaction.BlockNumber.Value()).
				WithTransactionHash(transaction.Hash.Value()).
				WithMetadata("contractAddress", contractAddress.Value())
		}

		contractAbi := contract.CombineAbis()

		// If the ABI is empty, return an error
		if contractAbi == "" {
			idx.Logger.Sugar().Errorw("No ABI found for contract", zap.String("hash", transaction.Hash.Value()))
			return nil, NewIndexError(IndexError_EmptyAbi, err).
				WithMessage("No ABI found for contract").
				WithBlockNumber(transaction.BlockNumber.Value()).
				WithTransactionHash(transaction.Hash.Value()).
				WithMetadata("contractAddress", contractAddress.Value())
		}
		a, err = idx.getAbi(contractAbi)
		if err != nil {
			idx.Logger.Sugar().Errorw(fmt.Sprintf("Failed to parse ABI for contract %s", contractAddress), zap.Error(err))
			return nil, NewIndexError(IndexError_FailedToParseAbi, err).
				WithMessage("Failed to parse ABI").
				WithBlockNumber(transaction.BlockNumber.Value()).
				WithTransactionHash(transaction.Hash.Value()).
				WithMetadata("contractAddress", contractAddress.Value())
		}
	} else {
		idx.Logger.Sugar().Debugw("Base transaction is not interesting",
			zap.String("hash", transaction.Hash.Value()),
			zap.String("contractAddress", contractAddress.Value()),
		)
	}

	logs := make([]*parser.DecodedLog, 0)

	for i, lg := range receipt.Logs {
		if !idx.IsInterestingAddress(lg.Address.Value()) {
			continue
		}
		decodedLog, err := idx.DecodeLogWithAbi(a, receipt, lg)
		if err != nil {
			msg := fmt.Sprintf("Error decoding log - index: '%d' - '%s'", i, transaction.Hash.Value())
			idx.Logger.Sugar().Debugw(msg, zap.Error(err))
			return nil, NewIndexError(IndexError_FailedToDecodeLog, err).
				WithMessage(msg).
				WithBlockNumber(transaction.BlockNumber.Value()).
				WithTransactionHash(transaction.Hash.Value()).
				WithMetadata("contractAddress", contractAddress.Value()).
				WithLogIndex(lg.LogIndex.Value())
		} else {
			idx.Logger.Sugar().Debugw(fmt.Sprintf("Decoded log - index: '%d' - '%s'", i, transaction.Hash.Value()), zap.Any("decodedLog", decodedLog))
		}

		logs = append(logs, decodedLog)
	}
	idx.Logger.Sugar().Debugw("Parsed interesting logs for transaction",
		zap.Int("count", len(logs)),
		zap.String("transactionHash", transaction.Hash.Value()),
	)
	parsedTransaction.Logs = logs
	return parsedTransaction, nil
}

// DecodeLogWithAbi decodes a log using the appropriate ABI.
// If the log address matches the transaction's target contract, it uses the provided ABI.
// Otherwise, it attempts to find the contract at the log address and use its ABI.
// This handles cases where a contract interacts with another contract that emits events.
// Returns the decoded log and any error encountered during decoding.
func (idx *Indexer) DecodeLogWithAbi(
	a *abi.ABI,
	txReceipt *ethereum.EthereumTransactionReceipt,
	lg *ethereum.EthereumEventLog,
) (*parser.DecodedLog, error) {
	logAddress := common.HexToAddress(lg.Address.Value())

	// If the address of the log is not the same as the contract address, we need to load the ABI for the log
	//
	// The typical case is when a contract interacts with another contract that emits an event
	if utils.AreAddressesEqual(logAddress.String(), txReceipt.GetTargetAddress().Value()) && a != nil {
		return idx.DecodeLog(a, lg)
	} else {
		idx.Logger.Sugar().Debugw("Log address does not match contract address", zap.String("logAddress", logAddress.String()), zap.String("contractAddress", txReceipt.GetTargetAddress().Value()))
		// Find/create the log address and attempt to determine if it is a proxy address
		foundContract, err := idx.ContractManager.GetContractWithProxy(logAddress.String(), txReceipt.BlockNumber.Value())
		if err != nil {
			return idx.DecodeLog(nil, lg)
		}
		if foundContract == nil {
			idx.Logger.Sugar().Debugw("No contract found for address", zap.String("address", logAddress.String()))
			return idx.DecodeLog(nil, lg)
		}

		contractAbi := foundContract.CombineAbis()
		if err != nil {
			idx.Logger.Sugar().Errorw("Failed to combine ABIs", zap.Error(err), zap.String("contractAddress", logAddress.String()))
			return idx.DecodeLog(nil, lg)
		}

		if contractAbi == "" {
			idx.Logger.Sugar().Debugw("No ABI found for contract", zap.String("contractAddress", logAddress.String()))
			return idx.DecodeLog(nil, lg)
		}

		// newAbi, err := abi.JSON(strings.NewReader(contractAbi))
		newAbi, err := idx.getAbi(contractAbi)
		if err != nil {
			idx.Logger.Sugar().Errorw("Failed to parse ABI",
				zap.Error(err),
				zap.String("contractAddress", logAddress.String()),
			)
			return idx.DecodeLog(nil, lg)
		}

		return idx.DecodeLog(newAbi, lg)
	}
}

// DecodeLog decodes a log using the provided ABI.
// It extracts the event name, arguments, and output data from the log.
// Returns the decoded log with structured event data and any error encountered during decoding.
// If no ABI is provided, returns an error.
func (idx *Indexer) DecodeLog(a *abi.ABI, lg *ethereum.EthereumEventLog) (*parser.DecodedLog, error) {
	idx.Logger.Sugar().Debugw(fmt.Sprintf("Decoding log with txHash: '%s' address: '%s'", lg.TransactionHash.Value(), lg.Address.Value()))
	logAddress := common.HexToAddress(lg.Address.Value())

	topicHash := common.Hash{}
	if len(lg.Topics) > 0 {
		// Handle case where the log has no topics
		// Original tx this failed on: https://holesky.etherscan.io/tx/0x044213f3e6c0bfa7721a1b6cc42a354096b54b20c52e4c7337fcfee09db80d90#eventlog
		topicHash = common.HexToHash(lg.Topics[0].Value())
	}

	decodedLog := &parser.DecodedLog{
		Address:  logAddress.String(),
		LogIndex: lg.LogIndex.Value(),
	}

	if a == nil {
		idx.Logger.Sugar().Errorw("No ABI provided for decoding log",
			zap.String("address", logAddress.String()),
		)
		return nil, errors.New("no ABI provided for decoding log")
	}

	event, err := a.EventByID(topicHash)
	if err != nil {
		idx.Logger.Sugar().Debugw(fmt.Sprintf("Failed to find event by ID '%s'", topicHash))
		return decodedLog, err
	}

	decodedLog.EventName = event.RawName
	decodedLog.Arguments = make([]parser.Argument, len(event.Inputs))

	for i, input := range event.Inputs {
		decodedLog.Arguments[i] = parser.Argument{
			Name:    input.Name,
			Type:    input.Type.String(),
			Indexed: input.Indexed,
		}
	}

	if len(lg.Topics) > 1 {
		for i, param := range lg.Topics[1:] {
			d, err := ParseLogValueForType(event.Inputs[i], param.Value())
			if err != nil {
				idx.Logger.Sugar().Errorw("Failed to parse log value for type", zap.Error(err))
			} else {
				decodedLog.Arguments[i].Value = d
			}
		}
	}

	if len(lg.Data) > 0 {
		// strip the leading 0x
		byteData, err := hex.DecodeString(lg.Data.Value()[2:])
		if err != nil {
			idx.Logger.Sugar().Errorw("Failed to decode data to bytes: ", err)
			return decodedLog, err
		}

		outputDataMap := make(map[string]interface{})
		err = a.UnpackIntoMap(outputDataMap, event.Name, byteData)
		if err != nil {
			idx.Logger.Sugar().Errorw("Failed to unpack data",
				zap.Error(err),
				zap.String("hash", lg.TransactionHash.Value()),
				zap.String("address", lg.Address.Value()),
				zap.String("eventName", event.Name),
				zap.String("transactionHash", lg.TransactionHash.Value()),
			)
			return nil, errors.New("failed to unpack data")
		}

		decodedLog.OutputData = outputDataMap
	}
	return decodedLog, nil
}

// ParseLogValueForType converts an Ethereum log value to an appropriate Go type
// based on the ABI argument type.
// It handles integer, boolean, address, string, and byte types.
// Returns the converted value and any error encountered during conversion.
func ParseLogValueForType(argument abi.Argument, value string) (interface{}, error) {
	valueBytes, _ := hexutil.Decode(value)
	switch argument.Type.T {
	case abi.IntTy, abi.UintTy:
		return abi.ReadInteger(argument.Type, valueBytes)
	case abi.BoolTy:
		return readBool(valueBytes)
	case abi.AddressTy:
		return common.HexToAddress(value), nil
	case abi.StringTy:
		return value, nil
	case abi.BytesTy, abi.FixedBytesTy:
		// return value as-is; hex encoded string
		return value, nil
	default:
		return value, nil
	}
}

// errBadBool is returned when a boolean value in an Ethereum log is improperly encoded.
var (
	errBadBool = errors.New("abi: improperly encoded boolean value")
)

// readBool converts a 32-byte word to a boolean value.
// Valid encodings have all bytes except the last one set to zero,
// and the last byte set to either 0 (false) or 1 (true).
// Returns the boolean value and an error if the encoding is invalid.
func readBool(word []byte) (bool, error) {
	for _, b := range word[:31] {
		if b != 0 {
			return false, errBadBool
		}
	}
	switch word[31] {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, errBadBool
	}
}
