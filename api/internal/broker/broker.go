// Package broker abstracts the partitioned log the scorers consume (Redis Streams or Redpanda).
package broker

import (
	"context"
	"time"
)

type Message struct {
	Key     string
	Payload []byte
}

type Delivery struct {
	Partition   int
	ID          string
	Payload     []byte
	Redelivered bool // true when this message was handed out before and never acked
}

// Broker is an at-least-once partitioned log with one consumer group.
type Broker interface {
	// Setup creates streams/topics and the consumer group; safe to call repeatedly.
	Setup(ctx context.Context) error
	Publish(ctx context.Context, partition int, msg Message) error
	// Consume returns up to max new messages for consumer, waiting briefly when none are ready.
	Consume(ctx context.Context, partition int, consumer string, max int) ([]Delivery, error)
	Ack(ctx context.Context, ds ...Delivery) error
	// Reclaim moves other consumers' unacked messages that have been idle for minIdle to consumer.
	// Adapters without per-message ownership may return nothing and redeliver through Consume.
	Reclaim(ctx context.Context, partition int, consumer string, minIdle time.Duration) ([]Delivery, error)
	// Pending counts unacked messages on partition held by consumers other than except.
	Pending(ctx context.Context, partition int, except string) (int64, error)
	DeadLetter(ctx context.Context, d Delivery, reason string) error
}
