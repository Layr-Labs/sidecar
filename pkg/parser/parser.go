// Package parser provides types and functionality for parsing and decoding
// Ethereum transactions and event logs. It helps transform raw blockchain data
// into structured, typed representations.
package parser

import (
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
)

// ParsedTransaction represents a decoded Ethereum transaction with its method name,
// decoded input data, logs, and references to the original transaction and receipt.
type ParsedTransaction struct {
	// MethodName is the name of the contract method that was called
	MethodName string
	// DecodedData is the human-readable representation of the transaction input data
	DecodedData string
	// Logs contains the decoded event logs emitted by the transaction
	Logs []*DecodedLog
	// Transaction is a reference to the original Ethereum transaction
	Transaction *ethereum.EthereumTransaction
	// Receipt is a reference to the transaction receipt
	Receipt *ethereum.EthereumTransactionReceipt
}

// DecodedLog represents a decoded Ethereum event log with its arguments and metadata.
// It contains the event name, emitting contract address, and structured argument data.
type DecodedLog struct {
	// LogIndex is the position of the log in the block
	LogIndex uint64
	// Address is the contract address that emitted the event
	Address string
	// Arguments contains the decoded event parameters
	Arguments []Argument
	// EventName is the name of the emitted event
	EventName string
	// OutputData contains the decoded event data as a map
	OutputData map[string]interface{}
}

// Argument represents a single parameter in a decoded event log or function call.
// It includes the parameter name, type, value, and whether it was indexed in the event.
type Argument struct {
	// Name is the parameter name
	Name string
	// Type is the Solidity type of the parameter
	Type string
	// Value is the actual parameter value
	Value interface{}
	// Indexed indicates whether this was an indexed event parameter
	Indexed bool
}
