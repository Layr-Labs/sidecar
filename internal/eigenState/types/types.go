package types

import (
	"github.com/Layr-Labs/go-sidecar/internal/storage"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type StateRoot string

type IBaseEigenStateModel interface {
	// GetLogger
	// Get the logger for the model
	Logger() *zap.Logger

	// GetModelName
	// Get the name of the model
	ModelName() string

	// GetTableName
	// Get the name of the table for the model
	TableName() string

	// GetDB
	// Get the database connection for the model
	DB() *gorm.DB

	// Base
	// Base model for the state model
	// used for testing
	Base() interface{}

	// GetInterestingLogMap
	// Get the map of interesting logs for the model
	GetInterestingLogMap() map[string][]string

	// InitBlock
	// Perform any necessary setup for processing a block
	InitBlock(blockNumber uint64) error

	// CleanupBlock
	// Cleanup any state changes for the block
	CleanupBlock(blockNumber uint64) error

	// HandleStateChange
	// Allow the state model to handle the state change
	//
	// Returns the saved value. Listed as an interface because go generics suck
	HandleStateChange(log *storage.TransactionLog) (interface{}, error)

	// CommitFinalState
	// Once all state changes are processed, commit the final state to the database
	CommitFinalState(blockNumber uint64) error

	// GetStateDiffs
	// Get the state diffs for the model
	GetStateDiffs(blockNumber uint64) ([]StateDiff, error)
}

type IEigenStateModel interface {
	IBaseEigenStateModel

	// IsInterestingLog
	// Determine if the log is interesting to the state model
	IsInterestingLog(log *storage.TransactionLog) bool

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
type StateTransitions[T interface{}] map[uint64]func(log *storage.TransactionLog) (*T, error)

type SlotID string

type StateDiff struct {
	SlotID SlotID
	Value  []byte
}
