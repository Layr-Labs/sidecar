package rewardsCalculatorQueue

import "fmt"

func (rcq *RewardsCalculatorQueue) Process() {
	for {
		select {
		case <-rcq.done:
			rcq.logger.Sugar().Infow("Closing rewards calculation queue")
			return
		case msg := <-rcq.queue:
			rcq.logger.Sugar().Infow("Processing rewards calculation message", "data", msg.Data)
			rcq.processMessage(msg)
		}
	}
}

func (rcq *RewardsCalculatorQueue) processMessage(msg *RewardsCalculationMessage) {
	response := &RewardsCalculatorResponse{}
	cutoffDate := msg.Data.CutoffDate

	switch msg.Data.CalculationType {
	case RewardsCalculationType_CalculateRewards:
		if cutoffDate == "" || cutoffDate == "latest" {
			cutoffDateUsed, err := rcq.rewardsCalculator.CalculateRewardsForLatestSnapshot()
			response.Error = err
			response.Data = &RewardsCalculatorResponseData{CutoffDate: cutoffDateUsed}
		} else {
			response.Error = rcq.rewardsCalculator.CalculateRewardsForSnapshotDate(msg.Data.CutoffDate)
			response.Data = &RewardsCalculatorResponseData{CutoffDate: msg.Data.CutoffDate}
		}
	case RewardsCalculationType_BackfillStakerOperators:
		response.Error = rcq.rewardsCalculator.BackfillAllStakerOperators()
		response.Data = &RewardsCalculatorResponseData{}
	case RewardsCalculationType_BackfillStakerOperatorsSnapshot:
		if cutoffDate == "" {
			response.Error = fmt.Errorf("cutoffDate date is required")
			break
		}
		response.Error = rcq.rewardsCalculator.GenerateStakerOperatorsTableForPastSnapshot(msg.Data.CutoffDate)
		response.Data = &RewardsCalculatorResponseData{}
	default:
		response.Error = fmt.Errorf("unknown calculation type %s", msg.Data.CalculationType)
	}
	if msg.ResponseChan == nil {
		rcq.logger.Sugar().Errorw("No response channel for rewards calculation message", "data", msg.Data)
		return
	}
	msg.ResponseChan <- response
}
