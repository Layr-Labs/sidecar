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

type ExecutorOperatorSetRegistered struct {
	Caller                string
	Avs                   string
	ExecutorOperatorSetId string
	IsRegistered          bool
	TransactionHash       string
	BlockNumber           uint64
	LogIndex              uint64
}

func (*ExecutorOperatorSetRegistered) TableName() string {
	return "executor_operator_set_registered"
}

type TaskCreated struct {
	Creator               string
	TaskHash              string
	Avs                   string
	ExecutorOperatorSetId uint32
	RefundCollector       string
	AvsFee                string
	TaskDeadline          string
	Payload               string
	TransactionHash       string
	BlockNumber           uint64
	LogIndex              uint64
}

func (*TaskCreated) TableName() string {
	return "task_created"
}

type TaskVerified struct {
	Aggregator            string
	TaskHash              string
	Avs                   string
	ExecutorOperatorSetId uint32
	ExecutorCert          string
	Result                string
	TransactionHash       string
	BlockNumber           uint64
	LogIndex              uint64
}

func (*TaskVerified) TableName() string {
	return "task_verified"
}
