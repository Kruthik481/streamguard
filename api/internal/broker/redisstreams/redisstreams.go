// Package redisstreams implements broker.Broker on Redis Streams consumer groups.
package redisstreams

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Kruthik481/streamguard/api/internal/broker"
)

const (
	group     = "scorers"
	maxLen    = 5000 // per partition; bounds memory on the 250 MB free tier
	dlqMaxLen = 1000
	block     = 500 * time.Millisecond
)

type Broker struct {
	rdb        redis.UniversalClient
	prefix     string
	partitions int
}

func New(rdb redis.UniversalClient, prefix string, partitions int) *Broker {
	return &Broker{rdb: rdb, prefix: prefix, partitions: partitions}
}

func (b *Broker) stream(p int) string { return fmt.Sprintf("%stx:%d", b.prefix, p) }

func (b *Broker) Setup(ctx context.Context) error {
	for p := range b.partitions {
		err := b.rdb.XGroupCreateMkStream(ctx, b.stream(p), group, "0").Err()
		if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
			return fmt.Errorf("create group on %s: %w", b.stream(p), err)
		}
	}
	return nil
}

func (b *Broker) Publish(ctx context.Context, p int, msg broker.Message) error {
	return b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: b.stream(p), MaxLen: maxLen, Approx: true,
		Values: map[string]any{"k": msg.Key, "m": msg.Payload},
	}).Err()
}

func (b *Broker) Consume(ctx context.Context, p int, consumer string, max int) ([]broker.Delivery, error) {
	res, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: consumer, Streams: []string{b.stream(p), ">"}, Count: int64(max), Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []broker.Delivery
	for _, s := range res {
		for _, m := range s.Messages {
			out = append(out, delivery(p, m, false))
		}
	}
	return out, nil
}

func (b *Broker) Ack(ctx context.Context, ds ...broker.Delivery) error {
	byPartition := map[int][]string{}
	for _, d := range ds {
		byPartition[d.Partition] = append(byPartition[d.Partition], d.ID)
	}
	for p, ids := range byPartition {
		if err := b.rdb.XAck(ctx, b.stream(p), group, ids...).Err(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) Reclaim(ctx context.Context, p int, consumer string, minIdle time.Duration) ([]broker.Delivery, error) {
	msgs, _, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: b.stream(p), Group: group, Consumer: consumer, MinIdle: minIdle, Start: "0-0", Count: 100,
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]broker.Delivery, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, delivery(p, m, true))
	}
	return out, nil
}

func (b *Broker) Pending(ctx context.Context, p int, except string) (int64, error) {
	res, err := b.rdb.XPending(ctx, b.stream(p), group).Result()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return res.Count - res.Consumers[except], nil
}

func (b *Broker) DeadLetter(ctx context.Context, d broker.Delivery, reason string) error {
	return b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: b.prefix + "dlq", MaxLen: dlqMaxLen, Approx: true,
		Values: map[string]any{"partition": d.Partition, "id": d.ID, "m": d.Payload, "reason": reason},
	}).Err()
}

func delivery(p int, m redis.XMessage, redelivered bool) broker.Delivery {
	payload, _ := m.Values["m"].(string)
	return broker.Delivery{Partition: p, ID: m.ID, Payload: []byte(payload), Redelivered: redelivered}
}
