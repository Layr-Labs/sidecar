package rewardsCalculatorQueue

import (
	"context"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"go.uber.org/zap"
)

// NewRewardsCalculatorQueue creates a new RewardsCalculatorQueue instance.
// It initializes the internal channels and returns a queue ready for processing
// rewards calculation requests.
//
// Parameters:
//   - rc: The rewards calculator that will perform the actual calculations
//   - logger: Logger for recording operations and errors
//
// Returns:
//   - *RewardsCalculatorQueue: A new queue instance ready for use
func NewRewardsCalculatorQueue(rc *rewards.RewardsCalculator, logger *zap.Logger) *RewardsCalculatorQueue {
	queue := &RewardsCalculatorQueue{
		logger:            logger,
		rewardsCalculator: rc,
		// allow the queue to buffer up to 100 messages
		queue: make(chan *RewardsCalculationMessage, 100),
		done:  make(chan struct{}),
	}
	return queue
}

// Enqueue adds a new message to the queue and returns immediately without waiting
// for the calculation to complete. This is useful for fire-and-forget operations
// or when using a custom response channel.
//
// Parameters:
//   - payload: The calculation message to enqueue
func (rcq *RewardsCalculatorQueue) Enqueue(payload *RewardsCalculationMessage) {
	rcq.logger.Sugar().Infow("Enqueueing rewards calculation message", "data", payload.Data)
	rcq.queue <- payload
}

// EnqueueAndWait adds a new message to the queue and waits for the calculation to complete
// or for the context to be canceled. This is a blocking operation that returns the
// calculation result or an error.
//
// Parameters:
//   - ctx: Context for cancellation and timeouts
//   - data: The calculation data to process
//
// Returns:
//   - *RewardsCalculatorResponseData: The calculation result, if successful
//   - error: Any error that occurred during calculation or if the context was canceled
func (rcq *RewardsCalculatorQueue) EnqueueAndWait(ctx context.Context, data RewardsCalculationData) (*RewardsCalculatorResponseData, error) {
	responseChan := make(chan *RewardsCalculatorResponse, 1)

	payload := &RewardsCalculationMessage{
		Data:         data,
		ResponseChan: responseChan,
	}
	rcq.Enqueue(payload)

	rcq.logger.Sugar().Infow("Waiting for rewards calculation response", "data", data)

	select {
	case response := <-responseChan:
		rcq.logger.Sugar().Infow("Received rewards calculation response")
		return response.Data, response.Error
	case <-ctx.Done():
		rcq.logger.Sugar().Infow("Received context.Done()")
		return nil, ctx.Err()
	}
}

// Close signals the queue to stop processing messages and releases resources.
// This should be called when the queue is no longer needed to prevent goroutine leaks.
func (rcq *RewardsCalculatorQueue) Close() {
	rcq.logger.Sugar().Infow("Closing rewards calculation queue")
	close(rcq.done)
}
