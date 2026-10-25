// Package brokertest is the behaviour every broker adapter must share.
package brokertest

import (
	"testing"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/broker"
)

const MinIdle = 100 * time.Millisecond

func Run(t *testing.T, newBroker func(t *testing.T) broker.Broker) {
	t.Run("delivers in order and acks", func(t *testing.T) {
		b := newBroker(t)
		publish(t, b, 0, "a", "b", "c")
		ds := consume(t, b, 0, "w0", 3)
		for i, want := range []string{"a", "b", "c"} {
			if string(ds[i].Payload) != want || ds[i].Redelivered || ds[i].Partition != 0 {
				t.Fatalf("delivery %d = %+v", i, ds[i])
			}
		}
		ack(t, b, ds...)
		expectPending(t, b, 0, "other", 0)
	})

	t.Run("keeps partitions apart", func(t *testing.T) {
		b := newBroker(t)
		publish(t, b, 0, "x")
		publish(t, b, 1, "y")
		ds := consume(t, b, 1, "w1", 1)
		if string(ds[0].Payload) != "y" || ds[0].Partition != 1 {
			t.Fatalf("got %+v", ds[0])
		}
	})

	t.Run("redelivers unacked messages to the next owner", func(t *testing.T) {
		b := newBroker(t)
		publish(t, b, 0, "a")
		consume(t, b, 0, "w0", 1) // w0 dies without acking
		n, err := b.Pending(t.Context(), 0, "w1")
		if err != nil || n < 1 {
			t.Fatalf("pending for others = %d, %v", n, err)
		}
		got := takeOver(t, b, 0, "w1", 1)
		if string(got[0].Payload) != "a" || !got[0].Redelivered {
			t.Fatalf("got %+v", got[0])
		}
		ack(t, b, got...)
		expectPending(t, b, 0, "w1", 0)
	})

	t.Run("does not redeliver acked messages", func(t *testing.T) {
		b := newBroker(t)
		publish(t, b, 0, "a")
		ack(t, b, consume(t, b, 0, "w0", 1)...)
		publish(t, b, 0, "b")
		got := takeOver(t, b, 0, "w1", 1)
		if string(got[0].Payload) != "b" || got[0].Redelivered {
			t.Fatalf("got %+v", got[0])
		}
	})

	t.Run("dead letters", func(t *testing.T) {
		b := newBroker(t)
		publish(t, b, 2, "garbage")
		ds := consume(t, b, 2, "w2", 1)
		if err := b.DeadLetter(t.Context(), ds[0], "undecodable"); err != nil {
			t.Fatal(err)
		}
	})
}

func publish(t *testing.T, b broker.Broker, p int, payloads ...string) {
	t.Helper()
	for _, s := range payloads {
		if err := b.Publish(t.Context(), p, broker.Message{Key: "k", Payload: []byte(s)}); err != nil {
			t.Fatal(err)
		}
	}
}

func ack(t *testing.T, b broker.Broker, ds ...broker.Delivery) {
	t.Helper()
	if err := b.Ack(t.Context(), ds...); err != nil {
		t.Fatal(err)
	}
}

func consume(t *testing.T, b broker.Broker, p int, consumer string, n int) []broker.Delivery {
	t.Helper()
	return collect(t, n, func() ([]broker.Delivery, error) { return b.Consume(t.Context(), p, consumer, n) })
}

// takeOver is what a new partition owner does: reclaim first, then read.
func takeOver(t *testing.T, b broker.Broker, p int, consumer string, n int) []broker.Delivery {
	t.Helper()
	return collect(t, n, func() ([]broker.Delivery, error) {
		ds, err := b.Reclaim(t.Context(), p, consumer, MinIdle)
		if err != nil || len(ds) > 0 {
			return ds, err
		}
		return b.Consume(t.Context(), p, consumer, n)
	})
}

func collect(t *testing.T, n int, next func() ([]broker.Delivery, error)) []broker.Delivery {
	t.Helper()
	var got []broker.Delivery
	deadline := time.Now().Add(10 * time.Second)
	for len(got) < n && time.Now().Before(deadline) {
		ds, err := next()
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ds...)
		if len(ds) == 0 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	if len(got) < n {
		t.Fatalf("got %d deliveries, want %d", len(got), n)
	}
	return got
}

func expectPending(t *testing.T, b broker.Broker, p int, except string, want int64) {
	t.Helper()
	n, err := b.Pending(t.Context(), p, except)
	if err != nil || n != want {
		t.Fatalf("pending = %d, %v; want %d", n, err, want)
	}
}
