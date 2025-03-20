package types

type IStateMigration interface {
	// MigrateState migrates the state from any state previous to the currentBlockNumber and
	// returns a list of roots that represent the changes for each block processed
	MigrateState(currentBlockNumber uint64) ([][]byte, map[string][]interface{}, error)

	GetMigrationName() string
}

type IStateMigrator interface {
	RunMigrationsForBlock(blockNumber uint64) ([]byte, map[string][]interface{}, error)
}
