package storage

import (
	"github.com/Layr-Labs/sidecar/pkg/parser"
	"time"
)

type BlockStore interface {
	InsertBlockAtHeight(blockNumber uint64, hash string, parentHash string, blockTime uint64) (*Block, error)
	InsertBlockTransaction(blockNumber uint64, txHash string, txIndex uint64, from string, to string, contractAddress string, bytecodeHash string) (*Transaction, error)
	InsertTransactionLog(txHash string, transactionIndex uint64, blockNumber uint64, log *parser.DecodedLog, outputData map[string]interface{}) (*TransactionLog, error)
	GetLatestBlock() (*Block, error)
	GetBlockByNumber(blockNumber uint64) (*Block, error)
	InsertOperatorRestakedStrategies(avsDirectorAddress string, blockNumber uint64, blockTime time.Time, operator string, avs string, strategy string) (*OperatorRestakedStrategies, error)
	BulkInsertOperatorRestakedStrategies(operatorRestakedStrategies []*OperatorRestakedStrategies) ([]*OperatorRestakedStrategies, error)

	// Less generic functions
	GetLatestActiveAvsOperators(blockNumber uint64, avsDirectoryAddress string) ([]*ActiveAvsOperator, error)

	// DeleteCorruptedState deletes all the corrupted state from the database
	//
	// @param startBlockNumber: The block number from which to start (inclusive)
	// @param endBlockNumber: The block number at which to end (inclusive). If 0, it will delete all the corrupted state from the startBlock
	DeleteCorruptedState(startBlockNumber uint64, endBlockNumber uint64) error
}

// Tables.
type Block struct {
	Number     uint64
	Hash       string
	ParentHash string
	BlockTime  time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
	DeletedAt  time.Time
}

type Transaction struct {
	BlockNumber      uint64
	TransactionHash  string
	TransactionIndex uint64
	FromAddress      string
	ToAddress        string
	ContractAddress  string
	BytecodeHash     string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	DeletedAt        time.Time
}

type TransactionLog struct {
	TransactionHash  string
	TransactionIndex uint64
	BlockNumber      uint64
	Address          string
	Arguments        string
	EventName        string
	LogIndex         uint64
	OutputData       string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	DeletedAt        time.Time
}

type BatchTransaction struct {
	TxHash          string
	TxIndex         uint64
	From            string
	To              string
	ContractAddress string
	BytecodeHash    string
}

type OperatorRestakedStrategies struct {
	AvsDirectoryAddress string
	BlockNumber         uint64
	Operator            string
	Avs                 string
	Strategy            string
	BlockTime           time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
	DeletedAt           time.Time
}

type ExcludedAddresses struct {
	Address     string
	Network     string
	Description string
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeletedAt   time.Time
}

type RewardSnapshotStatus string

func (r RewardSnapshotStatus) String() string {
	return string(r)
}

var (
	RewardSnapshotStatusProcessing RewardSnapshotStatus = "processing"
	RewardSnapshotStatusCompleted  RewardSnapshotStatus = "complete"
	RewardSnapshotStatusFailed     RewardSnapshotStatus = "failed"
)

type GeneratedRewardsSnapshots struct {
	Id           uint64 `gorm:"type:serial"`
	SnapshotDate string
	Status       string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Not tables

type ActiveAvsOperator struct {
	Avs      string
	Operator string
}
