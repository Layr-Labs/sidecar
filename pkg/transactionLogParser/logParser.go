package transactionLogParser

import (
	"encoding/hex"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/contractAbi"
	"github.com/Layr-Labs/sidecar/pkg/contractManager"
	"github.com/Layr-Labs/sidecar/pkg/contractStore"
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"github.com/Layr-Labs/sidecar/pkg/utils"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// InterestingLogQualifier defines an interface for determining if a contract address
// is of interest for log parsing.
type InterestingLogQualifier interface {
	// IsInterestingAddress determines if logs from the given address should be processed
	IsInterestingAddress(address string) bool
}

// TransactionLogParser handles the parsing and decoding of Ethereum transaction logs.
// It uses contract ABIs to decode event data into structured format.
type TransactionLogParser struct {
	logger                  *zap.Logger
	contractManager         *contractManager.ContractManager
	interestingLogQualifier InterestingLogQualifier
}

// NewTransactionLogParser creates a new TransactionLogParser with the provided dependencies.
//
// Parameters:
//   - logger: Logger for recording operations
//   - contractManager: Manager for contract ABIs and metadata
//   - interestingLogQualifier: Qualifier to determine which logs to process
//
// Returns:
//   - *TransactionLogParser: A configured transaction log parser
func NewTransactionLogParser(
	logger *zap.Logger,
	contractManager *contractManager.ContractManager,
	interestingLogQualifier InterestingLogQualifier,
) *TransactionLogParser {
	return &TransactionLogParser{
		logger:                  logger,
		contractManager:         contractManager,
		interestingLogQualifier: interestingLogQualifier,
	}
}

// ParseTransactionLogs parses all logs in a transaction receipt and returns decoded logs.
// Only logs from addresses determined to be interesting by the InterestingLogQualifier are processed.
//
// Parameters:
//   - receipt: The Ethereum transaction receipt containing logs to parse
//
// Returns:
//   - []*parser.DecodedLog: Array of decoded logs
//   - error: Any error encountered during parsing
func (tlp *TransactionLogParser) ParseTransactionLogs(receipt *ethereum.EthereumTransactionReceipt) ([]*parser.DecodedLog, error) {
	contractAddress := receipt.GetTargetAddress()

	if contractAddress.Value() == "" {
		tlp.logger.Sugar().Debug("No contract address found in transaction receipt",
			zap.String("transactionHash", receipt.TransactionHash.Value()),
			zap.Uint64("blockNumber", receipt.BlockNumber.Value()),
		)
		return nil, nil
	}

	var a *abi.ABI
	logs := make([]*parser.DecodedLog, 0)

	for i, lg := range receipt.Logs {
		if !tlp.interestingLogQualifier.IsInterestingAddress(lg.Address.Value()) {
			continue
		}

		decodedLog, err := tlp.DecodeLogWithAbi(a, receipt, lg)
		if err != nil {
			msg := fmt.Sprintf("Error decoding log - index: '%d' - '%s'", i, receipt.TransactionHash.Value())
			tlp.logger.Sugar().Errorw(msg,
				zap.String("logAddress", lg.Address.Value()),
				zap.String("transactionHash", lg.TransactionHash.Value()),
				zap.Uint64("blockNumber", receipt.BlockNumber.Value()),
				zap.Uint64("logIndex", lg.LogIndex.Value()),
				zap.Error(err),
			)
			return nil, err
		} else {
			tlp.logger.Sugar().Debugw(fmt.Sprintf("Decoded log - index: '%d' - '%s'", i, receipt.TransactionHash.Value()))
		}
		logs = append(logs, decodedLog)
	}
	return logs, nil
}

// DecodeLogWithAbi decodes a log using the appropriate ABI.
// If the log address doesn't match the transaction's target address, it attempts to find
// the correct contract ABI using the contract manager.
//
// Parameters:
//   - a: Optional ABI to use for decoding (can be nil)
//   - txReceipt: The transaction receipt containing context information
//   - lg: The specific log to decode
//
// Returns:
//   - *parser.DecodedLog: The decoded log with structured data
//   - error: Any error encountered during decoding
func (tlp *TransactionLogParser) DecodeLogWithAbi(
	a *abi.ABI,
	txReceipt *ethereum.EthereumTransactionReceipt,
	lg *ethereum.EthereumEventLog,
) (*parser.DecodedLog, error) {
	logAddress := common.HexToAddress(lg.Address.Value())

	// If the address of the log is not the same as the contract address, we need to load the ABI for the log
	//
	// The typical case is when a contract interacts with another contract that emits an event
	if utils.AreAddressesEqual(logAddress.String(), txReceipt.GetTargetAddress().Value()) && a != nil {
		return tlp.DecodeLog(a, lg)
	} else {
		tlp.logger.Sugar().Debugw("Log address does not match contract address", zap.String("logAddress", logAddress.String()), zap.String("contractAddress", txReceipt.GetTargetAddress().Value()))
		// Find/create the log address and attempt to determine if it is a proxy address
		foundContracts, err := tlp.contractManager.GetContractWithImplementations(logAddress.String())
		if err != nil {
			return tlp.DecodeLog(nil, lg)
		}
		if len(foundContracts) == 0 {
			tlp.logger.Sugar().Debugw("No contract found for address", zap.String("address", logAddress.String()))
			return tlp.DecodeLog(nil, lg)
		}

		combinedAbis, err := contractStore.CombineAbis(foundContracts)
		if err != nil {
			tlp.logger.Sugar().Errorw("Failed to combine ABIs", zap.Error(err), zap.String("contractAddress", logAddress.String()))
			return tlp.DecodeLog(nil, lg)
		}

		if combinedAbis == "" {
			tlp.logger.Sugar().Debugw("No ABI found for contract", zap.String("contractAddress", logAddress.String()))
			return tlp.DecodeLog(nil, lg)
		}

		// newAbi, err := abi.JSON(strings.NewReader(combinedAbis))
		newAbi, err := contractAbi.UnmarshalJsonToAbi(combinedAbis, tlp.logger)
		if err != nil {
			tlp.logger.Sugar().Errorw("Failed to parse ABI",
				zap.Error(err),
				zap.String("contractAddress", logAddress.String()),
			)
			return tlp.DecodeLog(nil, lg)
		}

		return tlp.DecodeLog(newAbi, lg)
	}
}

// DecodeLog decodes a log using the provided ABI.
// It extracts the event name, arguments, and output data from the log.
// Returns the decoded log with structured event data and any error encountered during decoding.
// If no ABI is provided, returns an error.
//
// Parameters:
//   - a: The ABI to use for decoding
//   - lg: The log to decode
//
// Returns:
//   - *parser.DecodedLog: The decoded log with structured data
//   - error: Any error encountered during decoding
func (tlp *TransactionLogParser) DecodeLog(a *abi.ABI, lg *ethereum.EthereumEventLog) (*parser.DecodedLog, error) {
	tlp.logger.Sugar().Debugw(fmt.Sprintf("Decoding log with txHash: '%s' address: '%s'", lg.TransactionHash.Value(), lg.Address.Value()))
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
		tlp.logger.Sugar().Errorw("No ABI provided for decoding log",
			zap.String("address", logAddress.String()),
		)
		return nil, errors.New("no ABI provided for decoding log")
	}

	event, err := a.EventByID(topicHash)
	if err != nil {
		tlp.logger.Sugar().Debugw(fmt.Sprintf("Failed to find event by ID '%s'", topicHash))
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
				tlp.logger.Sugar().Errorw("Failed to parse log value for type", zap.Error(err))
			} else {
				decodedLog.Arguments[i].Value = d
			}
		}
	}

	if len(lg.Data) > 0 {
		// strip the leading 0x
		byteData, err := hex.DecodeString(lg.Data.Value()[2:])
		if err != nil {
			tlp.logger.Sugar().Errorw("Failed to decode data to bytes: ", err)
			return decodedLog, err
		}

		outputDataMap := make(map[string]interface{})
		err = a.UnpackIntoMap(outputDataMap, event.Name, byteData)
		if err != nil {
			tlp.logger.Sugar().Errorw("Failed to unpack data",
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
//
// Parameters:
//   - argument: The ABI argument definition containing type information
//   - value: The hex-encoded value to parse
//
// Returns:
//   - interface{}: The converted value
//   - error: Any error encountered during conversion
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
	errBadBool = fmt.Errorf("abi: improperly encoded boolean value")
)

// readBool converts a 32-byte word to a boolean value.
// Valid encodings have all bytes except the last one set to zero,
// and the last byte set to either 0 (false) or 1 (true).
//
// Parameters:
//   - word: The 32-byte array to convert
//
// Returns:
//   - bool: The decoded boolean value
//   - error: Error if the encoding is invalid
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
