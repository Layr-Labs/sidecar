package types

type RewardsClaimed struct {
	Root            string
	Earner          string
	Claimer         string
	Recipient       string
	Token           string
	ClaimedAmount   string
	TransactionHash string
	BlockNumber     uint64
	LogIndex        uint64
}

func (*RewardsClaimed) TableName() string {
	return "rewards_claimed"
}

type GenerationReservationCreated struct {
	Avs             string
	OperatorSetId   uint64
	TransactionHash string
	BlockNumber     uint64
	LogIndex        uint64
}

func (*GenerationReservationCreated) TableName() string {
	return "generation_reservation_created"
}

type GenerationReservationRemoved struct {
	Avs             string
	OperatorSetId   uint64
	TransactionHash string
	BlockNumber     uint64
	LogIndex        uint64
}

func (*GenerationReservationRemoved) TableName() string {
	return "generation_reservation_removed"
}
