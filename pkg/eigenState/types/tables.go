package types

import (
	"time"
)

type SubmittedDistributionRoot struct {
	Root                      string    `filter:"true"`
	BlockNumber               uint64    `filter:"true"`
	RootIndex                 uint64    `filter:"true"`
	RewardsCalculationEnd     time.Time `filter:"true"`
	RewardsCalculationEndUnit string    `filter:"true"`
	ActivatedAt               time.Time `filter:"true"`
	ActivatedAtUnit           string    `filter:"true"`
	CreatedAtBlockNumber      uint64    `filter:"true"`
	LogIndex                  uint64    `filter:"true"`
	TransactionHash           string    `filter:"true"`
}

func (sdr *SubmittedDistributionRoot) GetSnapshotDate() string {
	return sdr.RewardsCalculationEnd.UTC().Format(time.DateOnly)
}

type DisabledDistributionRoot struct {
	RootIndex       uint64 `filter:"true"`
	BlockNumber     uint64 `filter:"true"`
	LogIndex        uint64 `filter:"true"`
	TransactionHash string `filter:"true"`
}
