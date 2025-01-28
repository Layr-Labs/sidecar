package eventTypeRegistry

import (
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

	return reg, nil
}
