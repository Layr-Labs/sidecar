package types

import (
	"github.com/Layr-Labs/sidecar/pkg/storage"
)

type StateRoot string

type IEigenStateModel interface {
	// GetModelName
	// Get the name of the model
	GetModelName() string

	// IsInterestingLog
	// Determine if the log is interesting to the state model
	IsInterestingLog(log *storage.TransactionLog) bool

	// SetupStateForBlock
	// Perform any necessary setup for processing a block
	SetupStateForBlock(blockNumber uint64) error

	// CleanupProcessedStateForBlock
	// Perform any necessary cleanup for processing a block
	CleanupProcessedStateForBlock(blockNumber uint64) error

	// HandleStateChange
	// Allow the state model to handle the state change
	//
	// Returns the saved value. Listed as an interface because go generics suck
	HandleStateChange(log *storage.TransactionLog) (interface{}, error)

	// CommitFinalState
	// Once all state changes are processed, commit the final state to the database
	CommitFinalState(blockNumber uint64, ignoreInsertConflicts bool) error

	// GetCommittedState
	// Get the committed state for the model at the given block height.
	GetCommittedState(blockNumber uint64) ([]interface{}, error)

	// GenerateStateRoot
	// Generate the state root for the model
	GenerateStateRoot(blockNumber uint64) ([]byte, error)

	// DeleteState used to delete state stored that may be incomplete or corrupted
	// to allow for reprocessing of the state
	//
	// @param startBlockNumber the block number to start deleting state from (inclusive)
	// @param endBlockNumber the block number to end deleting state from (inclusive). If 0, delete all state from startBlockNumber
	DeleteState(startBlockNumber uint64, endBlockNumber uint64) error

	// ListForBlockRange lists all records for the block range, inclusive of start and end block numbers
	ListForBlockRange(startBlockNumber uint64, endBlockNumber uint64) ([]interface{}, error)

	IsActiveForBlockHeight(blockHeight uint64) (bool, error)

	GetTableName() string
}

type IEigenPrecommitProcessor interface {
	Process(blockNumber uint64, models map[string]IEigenStateModel) error
	GetName() string
}

// StateTransitions
// Map of block number to function that will transition the state to the next block.
type StateTransitions[T any] map[uint64]func(log *storage.TransactionLog) (T, error)

type SlotID string

type MerkleLeafPrefix []byte

var (
	MerkleLeafPrefix_Block            MerkleLeafPrefix = []byte("0x00")
	MerkleLeafPrefix_BlockHash        MerkleLeafPrefix = []byte("0x01")
	MerkleLeafPrefix_EigenStateRoot   MerkleLeafPrefix = []byte("0x02")
	MerkleLeafPrefix_EigenStateBlock  MerkleLeafPrefix = []byte("0x03")
	MerkleLeafPrefix_EigenStateChange MerkleLeafPrefix = []byte("0x04")
	MerkleLeafPrefix_MigrationRoot    MerkleLeafPrefix = []byte("0x05")
	MerkleLeafPrefix_MigratedState    MerkleLeafPrefix = []byte("0x06")
)
