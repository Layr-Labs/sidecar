package types

import (
	"time"
)

type SubmittedDistributionRoot struct {
	Root                      string
	BlockNumber               uint64
	RootIndex                 uint64
	RewardsCalculationEnd     time.Time
	RewardsCalculationEndUnit string
	ActivatedAt               time.Time
	ActivatedAtUnit           string
	CreatedAtBlockNumber      uint64
	LogIndex                  uint64
	TransactionHash           string
}

func (sdr *SubmittedDistributionRoot) GetSnapshotDate() string {
	return sdr.RewardsCalculationEnd.UTC().Format(time.DateOnly)
}

type DisabledDistributionRoot struct {
	RootIndex       uint64
	BlockNumber     uint64
	LogIndex        uint64
	TransactionHash string
}
