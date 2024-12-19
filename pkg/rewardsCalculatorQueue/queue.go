package rewardsCalculatorQueue

import (
	"context"
	"github.com/Layr-Labs/sidecar/pkg/rewards"
	"go.uber.org/zap"
)

// NewRewardsCalculatorQueue creates a new RewardsCalculatorQueue
func NewRewardsCalculatorQueue(rc *rewards.RewardsCalculator, logger *zap.Logger) *RewardsCalculatorQueue {
	queue := &RewardsCalculatorQueue{
		logger:            logger,
		rewardsCalculator: rc,
		// allow the queue to buffer up to 100 messages
		queue: make(chan *RewardsCalculationMessage, 100),
	}
	return queue
}

// Enqueue adds a new message to the queue and returns immediately
func (rcq *RewardsCalculatorQueue) Enqueue(payload *RewardsCalculationMessage) {
	rcq.logger.Sugar().Infow("Enqueueing rewards calculation message", "data", payload.Data)
	rcq.queue <- payload
}

// EnqueueAndWait adds a new message to the queue and waits for a response or returns if the context is done
func (rcq *RewardsCalculatorQueue) EnqueueAndWait(ctx context.Context, data RewardsCalculationData) (*RewardsCalculatorResponseData, error) {
	responseChan := make(chan *RewardsCalculatorResponse)

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

func (rcq *RewardsCalculatorQueue) Close() {
	rcq.logger.Sugar().Infow("Closing rewards calculation queue")
	close(rcq.done)
}
