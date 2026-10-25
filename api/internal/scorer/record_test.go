package scorer

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/domain"
)

// hangingStore models a half-open Postgres connection: Record returns only when its context ends.
type hangingStore struct{}

func (hangingStore) Record(ctx context.Context, _ domain.Tx, _ []domain.Hit, _ domain.Meta) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}

func TestKilledWorkerRecordIsBoundedByADeadline(t *testing.T) {
	old := recordTimeout
	recordTimeout = 50 * time.Millisecond
	t.Cleanup(func() { recordTimeout = old })
	c := New(DefaultConfig(), Deps{Store: hangingStore{}, Log: slog.New(slog.DiscardHandler)})
	killed, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.recordWithRetry(killed, domain.Tx{ID: "t1"}, nil, domain.Meta{})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("a timed-out record on a killed worker must report an error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("record on a killed worker hung; it holds the partition lock")
	}
}
