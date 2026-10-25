// Package redpanda implements broker.Broker on a Kafka-compatible topic with manual offset commits.
//
// Kafka has no per-message ownership, so one reader per partition stands in for it: a new consumer
// name, or Reclaim, restarts the partition's reader at the committed offset. Unacked messages then
// come back through Consume marked Redelivered. Ack commits the highest offset + 1. minIdle is unused.
package redpanda

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Kruthik481/streamguard/api/internal/broker"
)

const (
	group    = "scorers"
	pollWait = 500 * time.Millisecond
)

type reader struct {
	consumer string
	cl       *kgo.Client
}

type Broker struct {
	seeds      []string
	topic      string
	partitions int
	prod       *kgo.Client
	adm        *kadm.Client

	mu      sync.Mutex
	readers map[int]*reader
	highest map[int]int64 // highest offset ever handed out per partition; absent = none yet
}

func New(seeds []string, topic string, partitions int) (*Broker, error) {
	prod, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		return nil, err
	}
	return &Broker{
		seeds: seeds, topic: topic, partitions: partitions, prod: prod, adm: kadm.NewClient(prod),
		readers: map[int]*reader{}, highest: map[int]int64{},
	}, nil
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		r.cl.Close()
	}
	b.prod.Close()
}

func (b *Broker) Setup(ctx context.Context) error {
	for _, t := range []struct {
		name  string
		parts int32
	}{{b.topic, int32(b.partitions)}, {b.topic + ".dlq", 1}} {
		resp, err := b.adm.CreateTopic(ctx, t.parts, 1, nil, t.name)
		if err != nil {
			return err
		}
		if resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
			return resp.Err
		}
	}
	return nil
}

func (b *Broker) Publish(ctx context.Context, p int, msg broker.Message) error {
	return b.prod.ProduceSync(ctx, &kgo.Record{
		Topic: b.topic, Partition: int32(p), Key: []byte(msg.Key), Value: msg.Payload,
	}).FirstErr()
}

func (b *Broker) committed(ctx context.Context, p int) (int64, error) {
	offs, err := b.adm.FetchOffsets(ctx, group)
	if err != nil {
		return 0, err
	}
	if o, ok := offs.Lookup(b.topic, int32(p)); ok && o.At >= 0 {
		return o.At, nil
	}
	return 0, nil
}

// readerFor returns consumer's reader for p. When the partition changes hands, or reset is true,
// the old reader is closed and a new one starts at the committed offset.
func (b *Broker) readerFor(ctx context.Context, p int, consumer string, reset bool) (*reader, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r := b.readers[p]; r != nil {
		if r.consumer == consumer && !reset {
			return r, nil
		}
		r.cl.Close()
		delete(b.readers, p)
	}
	at, err := b.committed(ctx, p)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(b.seeds...), kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		b.topic: {int32(p): kgo.NewOffset().At(at)},
	}))
	if err != nil {
		return nil, err
	}
	r := &reader{consumer: consumer, cl: cl}
	b.readers[p] = r
	return r, nil
}

func (b *Broker) Consume(ctx context.Context, p int, consumer string, max int) ([]broker.Delivery, error) {
	r, err := b.readerFor(ctx, p, consumer, false)
	if err != nil {
		return nil, err
	}
	pctx, cancel := context.WithTimeout(ctx, pollWait)
	fetches := r.cl.PollRecords(pctx, max)
	cancel()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	for _, fe := range fetches.Errors() {
		if !errors.Is(fe.Err, context.DeadlineExceeded) && !errors.Is(fe.Err, context.Canceled) {
			return nil, fe.Err
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []broker.Delivery
	for _, rec := range fetches.Records() {
		top, seen := b.highest[p]
		out = append(out, broker.Delivery{
			Partition: p, ID: strconv.FormatInt(rec.Offset, 10), Payload: rec.Value,
			Redelivered: seen && rec.Offset <= top,
		})
		if !seen || rec.Offset > top {
			b.highest[p] = rec.Offset
		}
	}
	return out, nil
}

func (b *Broker) Ack(ctx context.Context, ds ...broker.Delivery) error {
	top := map[int]int64{}
	for _, d := range ds {
		off, err := strconv.ParseInt(d.ID, 10, 64)
		if err != nil {
			return err
		}
		if cur, ok := top[d.Partition]; !ok || off > cur {
			top[d.Partition] = off
		}
	}
	var offs kadm.Offsets
	for p, off := range top {
		offs.Add(kadm.Offset{Topic: b.topic, Partition: int32(p), At: off + 1, LeaderEpoch: -1})
	}
	resp, err := b.adm.CommitOffsets(ctx, group, offs)
	if err != nil {
		return err
	}
	return resp.Error()
}

func (b *Broker) Reclaim(ctx context.Context, p int, consumer string, _ time.Duration) ([]broker.Delivery, error) {
	_, err := b.readerFor(ctx, p, consumer, true)
	return nil, err
}

func (b *Broker) Pending(ctx context.Context, p int, except string) (int64, error) {
	b.mu.Lock()
	r := b.readers[p]
	top, seen := b.highest[p]
	b.mu.Unlock()
	if !seen || (r != nil && r.consumer == except) {
		return 0, nil
	}
	at, err := b.committed(ctx, p)
	if err != nil {
		return 0, err
	}
	return max(0, top+1-at), nil
}

func (b *Broker) DeadLetter(ctx context.Context, d broker.Delivery, reason string) error {
	return b.prod.ProduceSync(ctx, &kgo.Record{
		Topic: b.topic + ".dlq", Partition: 0, Value: d.Payload,
		Headers: []kgo.RecordHeader{
			{Key: "reason", Value: []byte(reason)},
			{Key: "source", Value: []byte(strconv.Itoa(d.Partition) + "/" + d.ID)},
		},
	}).FirstErr()
}
