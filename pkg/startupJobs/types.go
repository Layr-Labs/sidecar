package startupJobs

type StartupJob interface {
}

type ISidecar interface {
	DeleteCorruptedState(startBlock, endBlock uint64) error
}
