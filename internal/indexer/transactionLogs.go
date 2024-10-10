package indexer

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"github.com/Layr-Labs/go-sidecar/internal/clients/ethereum"
	"github.com/Layr-Labs/go-sidecar/internal/parser"
	"github.com/Layr-Labs/go-sidecar/internal/utils"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"go.uber.org/zap"
)

func (idx *Indexer) getAbi(json string) (*abi.ABI, error) {
	a := &abi.ABI{}

	err := a.UnmarshalJSON([]byte(json))

	if err != nil {
		foundMatch := false
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
		// Ignore really common compilation error
		if !foundMatch {
			idx.Logger.Sugar().Warnw("Error unmarshaling abi json", zap.Error(err))
		}
	}

	return a, nil
}

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
	if contractAddress.Value() == "" {
		idx.Logger.Sugar().Debugw("No contract address found in receipt, skipping", zap.String("hash", transaction.Hash.Value()))
		return nil, nil
	}

	// Check if the transaction address is interesting.
	// It may be the case that the transaction isnt interesting, but it emitted an interesting log, in which case
	// the log address would be different than the transaction address
	var a *abi.ABI
	if idx.IsInterestingAddress(contractAddress.Value()) {
		contract, err := idx.ContractManager.FindOrCreateContractWithProxy(contractAddress.Value(), transaction.BlockNumber.Value(), receipt.GetBytecodeHash(), false)
		if err != nil {
			idx.Logger.Sugar().Errorw(fmt.Sprintf("Failed to get contract for address %s", contractAddress), zap.Error(err))
			return nil, NewIndexError(IndexError_FailedToFindContract, err).
				WithMessage("Failed to find contract").
				WithBlockNumber(transaction.BlockNumber.Value()).
				WithTransactionHash(transaction.Hash.Value()).
				WithMetadata("contractAddress", contractAddress.Value())
		}

		if contract == nil {
			idx.Logger.Sugar().Debugw("No contract found for address", zap.String("hash", transaction.Hash.Value()))
			return nil, nil
		}

		contractAbi := contract.CombineAbis()
		if err != nil {
			idx.Logger.Sugar().Errorw("Failed to combine ABIs")
			return nil, NewIndexError(IndexError_FailedToCombineAbis, err).
				WithMessage("Failed to combine ABIs").
				WithBlockNumber(transaction.BlockNumber.Value()).
				WithTransactionHash(transaction.Hash.Value())
		}

		if contractAbi == "" {
			idx.Logger.Sugar().Debugw("No ABI found for contract", zap.String("hash", transaction.Hash.Value()))
			return parsedTransaction, nil
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

		// First, attempt to decode the transaction input
		txInput := transaction.Input.Value()
		if len(txInput) >= 10 {
			var method *abi.Method
			decodedSig, err := hex.DecodeString(txInput[2:10])
			if err != nil {
				idx.Logger.Sugar().Errorw("Failed to decode signature")
			}

			if len(decodedSig) > 0 {
				method, err = a.MethodById(decodedSig)
				if err != nil {
					idx.Logger.Sugar().Debugw(fmt.Sprintf("Failed to find method by ID '%s'", common.BytesToHash(decodedSig).String()))
					parsedTransaction.MethodName = "unknown"
				} else {
					parsedTransaction.MethodName = method.RawName
					decodedData, err := hex.DecodeString(txInput[10:])
					if err != nil {
						idx.Logger.Sugar().Errorw("Failed to decode input data")
					} else {
						callMap := map[string]interface{}{}
						if err := method.Inputs.UnpackIntoMap(callMap, decodedData); err != nil {
							idx.Logger.Sugar().Errorw("Failed to unpack data")
						}
						callMapBytes, err := json.Marshal(callMap)
						if err != nil {
							idx.Logger.Sugar().Errorw("Failed to marshal callMap data", zap.String("hash", transaction.Hash.Value()))
						}
						parsedTransaction.DecodedData = string(callMapBytes)
					}
				}
			}
		} else {
			idx.Logger.Sugar().Debugw(fmt.Sprintf("Transaction input is empty %s", contractAddress))
		}
	}

	logs := make([]*parser.DecodedLog, 0)
	// Attempt to decode each transaction log
	idx.Logger.Sugar().Debugw(fmt.Sprintf("Decoding '%d' logs for transaction", len(receipt.Logs)),
		zap.String("transactionHash", transaction.Hash.Value()),
		zap.Uint64("blockNumber", transaction.BlockNumber.Value()),
	)

	for i, lg := range receipt.Logs {
		if !idx.IsInterestingAddress(lg.Address.Value()) {
			idx.Logger.Sugar().Debugw("Skipping log with non-interesting address",
				zap.String("address", contractAddress.Value()),
				zap.String("transactionHash", transaction.Hash.Value()),
				zap.Uint64("logIndex", lg.LogIndex.Value()),
			)
			continue
		}
		decodedLog, err := idx.DecodeLogWithAbi(a, receipt, lg)
		if err != nil {
			idx.Logger.Sugar().Debugw(fmt.Sprintf("Error decoding log - index: '%d' - '%s'", i, transaction.Hash.Value()), zap.Error(err))
		} else {
			idx.Logger.Sugar().Debugw(fmt.Sprintf("Decoded log - index: '%d' - '%s'", i, transaction.Hash.Value()), zap.Any("decodedLog", decodedLog))
		}

		logs = append(logs, decodedLog)
	}
	parsedTransaction.Logs = logs
	return parsedTransaction, nil
}

func (idx *Indexer) FindContractUpgradedLogs(parsedLogs []*parser.DecodedLog) []*parser.DecodedLog {
	contractUpgradedLogs := make([]*parser.DecodedLog, 0)
	for _, parsedLog := range parsedLogs {
		if parsedLog.EventName == "Upgraded" && idx.IsInterestingAddress(parsedLog.Address) {
			contractUpgradedLogs = append(contractUpgradedLogs, parsedLog)
		}
	}
	return contractUpgradedLogs
}

func (idx *Indexer) IndexContractUpgrade(blockNumber uint64, upgradedLog *parser.DecodedLog, reindexContract bool) {
	// the new address that the contract points to
	newProxiedAddress := ""

	// Check the arguments for the new address. EIP-1967 contracts include this as an argument.
	// Otherwise, we'll check the storage slot
	for _, arg := range upgradedLog.Arguments {
		if arg.Name == "implementation" && arg.Value != "" {
			newProxiedAddress = arg.Value.(common.Address).String()
		}
	}

	// check the storage slot at the provided block number of the transaction
	if newProxiedAddress == "" {
		storageValue, err := idx.Fetcher.GetContractStorageSlot(context.Background(), upgradedLog.Address, blockNumber)
		if err != nil || storageValue == "" {
			idx.Logger.Sugar().Errorw("Failed to get storage value", zap.Error(err))
		} else if len(storageValue) != 66 {
			idx.Logger.Sugar().Errorw("Invalid storage value", zap.String("storageValue", storageValue))
		} else {
			newProxiedAddress = storageValue[26:]
		}
	}

	if newProxiedAddress == "" {
		idx.Logger.Sugar().Debugw("No new proxied address found", zap.String("address", upgradedLog.Address))
		return
	}

	_, err := idx.ContractManager.CreateProxyContract(upgradedLog.Address, newProxiedAddress, blockNumber, reindexContract)
	if err != nil {
		idx.Logger.Sugar().Errorw("Failed to create proxy contract", zap.Error(err))
		return
	}
	idx.Logger.Sugar().Infow("Upgraded proxy contract", zap.String("contractAddress", upgradedLog.Address), zap.String("proxyContractAddress", newProxiedAddress))
}

func (idx *Indexer) IndexContractUpgrades(blockNumber uint64, upgradedContractLogs []*parser.DecodedLog, reindexContract bool) {
	for _, log := range upgradedContractLogs {
		idx.IndexContractUpgrade(blockNumber, log, reindexContract)
	}
}

// DecodeLogWithAbi determines if the provided contract ABI matches that of the log
// For example, if the target contract performs a token transfer, that token may emit an
// event that will be captured in the list of logs. That ABI however is different and will
// need to be loaded in order to decode the log.
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
		// TODO - need a way to get the bytecode hash
		// Find/create the log address and attempt to determine if it is a proxy address
		foundOrCreatedContract, err := idx.ContractManager.FindOrCreateContractWithProxy(logAddress.String(), txReceipt.BlockNumber.Value(), "", false)
		if err != nil {
			return idx.DecodeLog(nil, lg)
		}
		if foundOrCreatedContract == nil {
			idx.Logger.Sugar().Debugw("No contract found for address", zap.String("address", logAddress.String()))
			return idx.DecodeLog(nil, lg)
		}

		contractAbi := foundOrCreatedContract.CombineAbis()
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

// DecodeLog will decode a log line using the provided abi.
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
		idx.Logger.Sugar().Debugw(fmt.Sprintf("No ABI provided, using topic hash as event name '%s'", logAddress.String()))
		decodedLog.EventName = topicHash.String()
		return decodedLog, nil
	}

	event, err := a.EventByID(topicHash)
	if err != nil {
		idx.Logger.Sugar().Debugw(fmt.Sprintf("Failed to find event by ID '%s'", topicHash))
		return decodedLog, err
	}

	decodedLog.EventName = event.RawName
	decodedLog.Arguments = make([]parser.Argument, len(event.Inputs))
	1
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
			idx.Logger.Sugar().Errorw("Failed to unpack data: ", err)
		}

		decodedLog.OutputData = outputDataMap
	}
	return decodedLog, nil
}

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

var (
	errBadBool = errors.New("abi: improperly encoded boolean value")
)

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
