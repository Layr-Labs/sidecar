package rewards

import (
	"time"
)

type CombinedRewards struct {
	Avs            string
	RewardHash     string
	Token          string
	Amount         string
	Strategy       string
	StrategyIndex  uint64
	Multiplier     string
	StartTimestamp time.Time
	EndTimestamp   time.Time
	Duration       uint64
	BlockNumber    uint64
	BlockDate      string
	BlockTime      time.Time
	RewardType     string // avs, all_stakers, all_earners
}

type OperatorAvsRegistrationSnapshots struct {
	Avs      string
	Operator string
	Snapshot time.Time
}

type OperatorAvsStrategySnapshot struct {
	Operator string
	Avs      string
	Strategy string
	Snapshot time.Time
}

type OperatorShareSnapshots struct {
	Operator string
	Strategy string
	Shares   string
	Snapshot time.Time
}

type StakerDelegationSnapshot struct {
	Staker   string
	Operator string
	Snapshot time.Time
}

type StakerShareSnapshot struct {
	Staker   string
	Strategy string
	Snapshot time.Time
	Shares   string
}

type StakerShares struct {
	Staker          string
	Strategy        string
	Shares          string
	StrategyIndex   uint64
	TransactionHash string
	LogIndex        uint64
	BlockTime       time.Time
	BlockDate       string
	BlockNumber     uint64
}

type OperatorShares struct {
	Operator        string
	Strategy        string
	Shares          string
	TransactionHash string
	LogIndex        uint64
	BlockNumber     uint64
	BlockTime       time.Time
	BlockDate       string
}

type DefaultOperatorSplitSnapshots struct {
	Split    uint64
	Snapshot time.Time
}

type OperatorAVSSplitSnapshots struct {
	Operator string
	Avs      string
	Split    uint64
	Snapshot time.Time
}

type OperatorPISplitSnapshots struct {
	Operator string
	Split    uint64
	Snapshot time.Time
}

type OperatorDirectedRewards struct {
	Avs             string
	RewardHash      string
	Token           string
	Operator        string
	OperatorIndex   uint64
	Amount          string
	Strategy        string
	StrategyIndex   uint64
	Multiplier      string
	StartTimestamp  *time.Time
	EndTimestamp    *time.Time
	Duration        uint64
	BlockNumber     uint64
	TransactionHash string
	LogIndex        uint64
}
