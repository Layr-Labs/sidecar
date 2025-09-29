// Package eventBusTypes defines the types and interfaces used by the eventBus package.
// It provides the core data structures for implementing a publish-subscribe pattern.
package eventBusTypes

import (
	"context"
	"sync"

	"github.com/Layr-Labs/sidecar/pkg/eigenState/stateManager"
	"github.com/Layr-Labs/sidecar/pkg/storage"
)

// EventName is a string type that identifies different types of events.
type EventName string

// String returns the string representation of the EventName.
func (en *EventName) String() string {
	return string(*en)
}

// Predefined event names used in the system.
var (
	// Event_BlockProcessed is emitted when a block has been fully processed.
	Event_BlockProcessed EventName = "block_processed"
)

// Event represents a message that is published to the event bus.
// It contains a name that identifies the type of event and arbitrary data.
type Event struct {
	// Name identifies the type of event
	Name EventName
	// Data contains the event payload, which can be of any type
	Data any
}

// ConsumerId is a string type that uniquely identifies an event consumer.
type ConsumerId string

// Consumer represents a subscriber to the event bus.
// It has a unique ID, a context for cancellation, and a channel for receiving events.
type Consumer struct {
	// Id uniquely identifies the consumer
	Id ConsumerId
	// Context can be used to signal cancellation
	Context context.Context
	// Channel receives events from the event bus
	Channel chan *Event
}

// ConsumerList is a thread-safe collection of consumers.
// It provides methods for adding, removing, and retrieving consumers.
type ConsumerList struct {
	mu        sync.Mutex
	consumers []*Consumer
}

// NewConsumerList creates a new empty ConsumerList.
func NewConsumerList() *ConsumerList {
	return &ConsumerList{
		consumers: make([]*Consumer, 0),
	}
}

// Add adds a consumer to the list in a thread-safe manner.
func (cl *ConsumerList) Add(consumer *Consumer) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.consumers = append(cl.consumers, consumer)
}

// Remove removes a consumer from the list in a thread-safe manner.
// It identifies the consumer by its ID.
func (cl *ConsumerList) Remove(consumer *Consumer) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	for i, c := range cl.consumers {
		if c.Id == consumer.Id {
			cl.consumers = append(cl.consumers[:i], cl.consumers[i+1:]...)
			break
		}
	}
}

// GetAll returns a copy of all consumers in the list in a thread-safe manner.
func (cl *ConsumerList) GetAll() []*Consumer {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.consumers
}

// IEventBus defines the interface for an event bus.
// It provides methods for subscribing, unsubscribing, and publishing events.
type IEventBus interface {
	// Subscribe registers a consumer to receive events
	Subscribe(consumer *Consumer)
	// Unsubscribe removes a consumer from the event bus
	Unsubscribe(consumer *Consumer)
	// Publish sends an event to all subscribed consumers
	Publish(event *Event)
}

// BlockProcessedData contains the data associated with a processed block.
// It is used as the payload for Event_BlockProcessed events.
type BlockProcessedData struct {
	// Block is the processed block
	Block *storage.Block
	// Transactions are the transactions in the block
	Transactions []*storage.Transaction
	// Logs are the transaction logs in the block
	Logs []*storage.TransactionLog
	// StateRoot contains the state root information
	StateRoot *stateManager.StateRoot
	// CommittedState contains the committed state changes
	CommittedState map[string][]interface{}
	// MetaCommittedState contains metaState changes (separate from eigenState)
	MetaCommittedState map[string][]interface{}
}
