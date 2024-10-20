package types

import (
	"github.com/Layr-Labs/go-sidecar/internal/storage"
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

	// HandleLog
	// Allow the state model to handle the state change
	//
	// Returns the saved value. Listed as an interface because go generics suck
	HandleLog(log *storage.TransactionLog) (interface{}, error)

	// CommitFinalState
	// Once all state changes are processed, commit the final state to the database
	CommitFinalState(blockNumber uint64) error

	// GenerateStateRoot
	// Generate the state root for the model
	GenerateStateRoot(blockNumber uint64) (StateRoot, error)

	// DeleteState used to delete state stored that may be incomplete or corrupted
	// to allow for reprocessing of the state
	//
	// @param startBlockNumber the block number to start deleting state from (inclusive)
	// @param endBlockNumber the block number to end deleting state from (inclusive). If 0, delete all state from startBlockNumber
	DeleteState(startBlockNumber uint64, endBlockNumber uint64) error
}

// StateTransitions
// Map of block number to function that will transition the state to the next block.
type StateTransitions map[uint64]func(log *storage.TransactionLog) (interface{}, error)

type SlotID string
