package eventTypeRegistry

import (
	"github.com/Layr-Labs/sidecar/pkg/eigenState/avsOperators"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/operatorShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/rewardSubmissions"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerDelegations"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stakerShares"
	"github.com/Layr-Labs/sidecar/pkg/eigenState/types"
	"github.com/Layr-Labs/sidecar/pkg/eventFilter"
	"github.com/Layr-Labs/sidecar/pkg/storage"
)

func BuildFilterableEventRegistry() (*eventFilter.FilterableRegistry, error) {
	reg := eventFilter.NewFilterableRegistry()
	if err := reg.RegisterType(&storage.Block{}); err != nil {
		return nil, err
	}
	if err := reg.RegisterType(&storage.Transaction{}); err != nil {
		return nil, err
	}
	if err := reg.RegisterType(&storage.TransactionLog{}); err != nil {
		return nil, err
	}
	if err := reg.RegisterType(&avsOperators.AvsOperatorStateChange{}); err != nil {
		return nil, err
	}
	if err := reg.RegisterType(&types.DisabledDistributionRoot{}); err != nil {
		return nil, err
	}
	if err := reg.RegisterType(&operatorShares.OperatorShareDeltas{}); err != nil {
		return nil, err
	}
	if err := reg.RegisterType(&rewardSubmissions.RewardSubmission{}); err != nil {
		return nil, err
	}
	if err := reg.RegisterType(&stakerDelegations.StakerDelegationChange{}); err != nil {
		return nil, err
	}
	if err := reg.RegisterType(&stakerShares.StakerShareDeltas{}); err != nil {
		return nil, err
	}
	if err := reg.RegisterType(&types.SubmittedDistributionRoot{}); err != nil {
		return nil, err
	}

	return reg, nil
}
