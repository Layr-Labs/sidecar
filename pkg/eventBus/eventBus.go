// Package eventBus provides a simple publish-subscribe mechanism for internal events.
// It allows components to communicate asynchronously through events without direct coupling.
package eventBus

import (
	"github.com/Layr-Labs/sidecar/pkg/eventBus/eventBusTypes"
	"go.uber.org/zap"
)

// EventBus implements a publish-subscribe pattern for distributing events to registered consumers.
// It maintains a list of consumers and provides methods for subscribing, unsubscribing, and publishing events.
type EventBus struct {
	// consumers is a thread-safe list of event consumers
	consumers *eventBusTypes.ConsumerList
	// logger is used for logging event operations
	logger *zap.Logger
}

// NewEventBus creates a new EventBus with the provided logger.
// It initializes an empty consumer list.
func NewEventBus(l *zap.Logger) *EventBus {
	return &EventBus{
		consumers: eventBusTypes.NewConsumerList(),
		logger:    l,
	}
}

// Subscribe registers a consumer to receive events published to the event bus.
// The consumer will receive events through its channel.
func (eb *EventBus) Subscribe(consumer *eventBusTypes.Consumer) {
	eb.consumers.Add(consumer)
}

// Unsubscribe removes a consumer from the event bus.
// The consumer will no longer receive events.
func (eb *EventBus) Unsubscribe(consumer *eventBusTypes.Consumer) {
	eb.consumers.Remove(consumer)
	eb.logger.Sugar().Infow("Unsubscribed consumer", zap.String("consumerId", string(consumer.Id)))
}

// Publish sends an event to all subscribed consumers.
// It attempts to send the event to each consumer's channel in a non-blocking way.
// If a consumer's channel is full or nil, the event is skipped for that consumer.
func (eb *EventBus) Publish(event *eventBusTypes.Event) {
	eb.logger.Sugar().Debugw("Publishing event", zap.String("eventName", string(event.Name)))
	for _, consumer := range eb.consumers.GetAll() {
		if consumer.Channel != nil {
			select {
			case consumer.Channel <- event:
				eb.logger.Sugar().Debugw("Published event to consumer",
					zap.String("consumerId", string(consumer.Id)),
					zap.String("eventName", event.Name.String()),
				)
			default:
				eb.logger.Sugar().Debugw("No receiver available, or channel is full",
					zap.String("consumerId", string(consumer.Id)),
					zap.String("eventName", event.Name.String()),
				)
			}
		} else {
			eb.logger.Sugar().Debugw("Consumer channel is nil", zap.String("consumerId", string(consumer.Id)))
		}
	}
}
