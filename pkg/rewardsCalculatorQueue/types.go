package rewardsCalculatorQueue

import (
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"go.uber.org/zap"
)

// RewardsCalculationType defines the type of rewards calculation operation to perform.
// It is used to determine which specific calculation function to execute.
type RewardsCalculationType string

// Predefined reward calculation types for different operations
var (
	// RewardsCalculationType_CalculateRewards indicates a request to calculate rewards for a specific cutoff date
	RewardsCalculationType_CalculateRewards RewardsCalculationType = "calculateRewards"

	// RewardsCalculationType_BackfillStakerOperators indicates a request to backfill all staker-operator relationships
	RewardsCalculationType_BackfillStakerOperators RewardsCalculationType = "backfillStakerOperators"

	// RewardsCalculationType_BackfillStakerOperatorsSnapshot indicates a request to backfill staker-operator relationships for a specific snapshot
	RewardsCalculationType_BackfillStakerOperatorsSnapshot RewardsCalculationType = "backfillStakerOperatorsSnapshot"
)

// RewardsCalculationData contains the parameters for a rewards calculation request.
// It specifies what type of calculation to perform and the cutoff date to use.
type RewardsCalculationData struct {
	// CalculationType determines which calculation operation to perform
	CalculationType RewardsCalculationType

	// CutoffDate is the date up to which to calculate rewards (format: YYYY-MM-DD)
	// If empty or "latest", the latest available date will be used
	CutoffDate string
}

// RewardsCalculationMessage represents a message in the rewards calculation queue.
// It contains the calculation request data and an optional channel for receiving the response.
type RewardsCalculationMessage struct {
	// Data contains the parameters for the calculation request
	Data RewardsCalculationData

	// ResponseChan is the channel where the calculation response will be sent
	// If nil, no response will be sent back
	ResponseChan chan *RewardsCalculatorResponse
}

// RewardsCalculatorResponseData contains the result data from a rewards calculation.
type RewardsCalculatorResponseData struct {
	// CutoffDate is the actual date used for the calculation
	CutoffDate string
}

// RewardsCalculatorResponse represents the complete response from a rewards calculation,
// including both the result data and any error that occurred.
type RewardsCalculatorResponse struct {
	// Data contains the result of the calculation
	Data *RewardsCalculatorResponseData

	// Error contains any error that occurred during calculation
	// If nil, the calculation was successful
	Error error
}

// RewardsCalculatorQueue manages asynchronous rewards calculation operations.
// It provides a queue-based interface for submitting calculation requests
// and receiving responses, allowing for non-blocking operation.
type RewardsCalculatorQueue struct {
	// logger for logging operations and errors
	logger *zap.Logger

	// rewardsCalculator performs the actual rewards calculations
	rewardsCalculator *rewards.RewardsCalculator

	// queue is the channel for receiving calculation requests
	queue chan *RewardsCalculationMessage

	// done is a channel for signaling shutdown of the queue
	done chan struct{}
}
