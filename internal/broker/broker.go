package broker

import "context"

// The simplest MVP message
type Message struct {
	Key   []byte
	Value []byte
}

// Broker is the core interface for the MVP message broker
type Broker interface {
	CreateTopic(ctx context.Context, name string, partitions int) error
	ListTopics(ctx context.Context) ([]string, error)

	Produce(ctx context.Context, topic string, msg Message) error
	Consume(ctx context.Context, topic string, group string) (<-chan Message, error)
}

// InMemoryBroker is our first implementation. For sure later we'll replace pieces with WAL, scheduler, partitions, etc
type InMemoryBroker struct {
	topics map[string][]Message
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		topics: make(map[string][]Message),
	}
}
