package store_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/store"
)

func TestStartRunRejectsAnActiveRun(t *testing.T) {
	s := newStore(t)
	now := time.Now().UTC().Truncate(time.Microsecond)
	r, err := s.StartRun(t.Context(), now, 3*time.Minute, 4)
	if err != nil || r.Status != "running" || !r.EndsAt.Equal(now.Add(3*time.Minute)) {
		t.Fatalf("run = %+v, %v", r, err)
	}
	_, err = s.StartRun(t.Context(), now.Add(time.Second), 3*time.Minute, 4)
	var active *store.ActiveError
	if !errors.As(err, &active) || !active.EndsAt.Equal(r.EndsAt) {
		t.Fatalf("want ActiveError, got %v", err)
	}
	if err := s.FinishRun(t.Context(), r.ID, "finished"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StartRun(t.Context(), now.Add(2*time.Second), time.Minute, 4); err != nil {
		t.Fatalf("after finish: %v", err)
	}
}

func TestStartRunEnforcesTheDailyCap(t *testing.T) {
	s := newStore(t)
	day := time.Date(2026, 10, 10, 1, 0, 0, 0, time.UTC)
	for i := range 2 {
		r, err := s.StartRun(t.Context(), day.Add(time.Duration(i)*time.Hour), time.Minute, 2)
		if err != nil {
			t.Fatal(err)
		}
		_ = s.FinishRun(t.Context(), r.ID, "finished")
	}
	if _, err := s.StartRun(t.Context(), day.Add(5*time.Hour), time.Minute, 2); !errors.Is(err, store.ErrDailyCap) {
		t.Fatalf("want ErrDailyCap, got %v", err)
	}
	if _, err := s.StartRun(t.Context(), day.Add(24*time.Hour), time.Minute, 2); err != nil {
		t.Fatalf("next UTC day must be allowed: %v", err)
	}
}

func TestStartRunIsExclusiveUnderConcurrency(t *testing.T) {
	s := newStore(t)
	now := time.Now().UTC()
	var wg sync.WaitGroup
	var mu sync.Mutex
	started := 0
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.StartRun(t.Context(), now, time.Minute, 10); err == nil {
				mu.Lock()
				started++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if started != 1 {
		t.Fatalf("%d runs started", started)
	}
}

func TestAbandonRunning(t *testing.T) {
	s := newStore(t)
	now := time.Now().UTC()
	if _, err := s.StartRun(t.Context(), now, time.Hour, 4); err != nil {
		t.Fatal(err)
	}
	n, err := s.AbandonRunning(t.Context())
	if err != nil || n != 1 {
		t.Fatalf("abandoned %d, %v", n, err)
	}
	if _, err := s.StartRun(t.Context(), now.Add(time.Second), time.Hour, 4); err != nil {
		t.Fatalf("start after abandon: %v", err)
	}
}
