package pipeline

import (
	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/eventBus/eventBusTypes"
	"github.com/Layr-Labs/sidecar/pkg/storage"
)

// HandleBlockProcessedHook publishes a BlockProcessed event to the event bus when a block
// has been fully processed by the pipeline. This allows other components to react to
// completed block processing.
//
// Parameters:
//   - block: The processed block data
//   - transactions: The transactions contained in the block
//   - logs: Transaction logs associated with the block
//   - stateRoot: The state root after applying the block
//   - committedState: Map of committed state changes resulting from this block
func (p *Pipeline) HandleBlockProcessedHook(
	block *storage.Block,
	transactions []*storage.Transaction,
	logs []*storage.TransactionLog,
	stateRoot *stateManager.StateRoot,
	committedState map[string][]interface{},
	metaCommittedState map[string][]interface{},
) {
	p.eventBus.Publish(&eventBusTypes.Event{
		Name: eventBusTypes.Event_BlockProcessed,
		Data: &eventBusTypes.BlockProcessedData{
			Block:              block,
			Transactions:       transactions,
			Logs:               logs,
			StateRoot:          stateRoot,
			CommittedState:     committedState,
			MetaCommittedState: metaCommittedState,
		},
	})
}
