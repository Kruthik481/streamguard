package run

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/domain"
	"github.com/Kruthik481/streamguard/api/internal/gen"
	"github.com/Kruthik481/streamguard/api/internal/hub"
	"github.com/Kruthik481/streamguard/api/internal/scorer"
	"github.com/Kruthik481/streamguard/api/internal/store"
)

type fakeStore struct {
	mu       sync.Mutex
	err      error
	started  int64
	finished map[int64]string
}

func (s *fakeStore) StartRun(_ context.Context, now time.Time, dur time.Duration, _ int) (store.Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return store.Run{}, s.err
	}
	s.started++
	return store.Run{ID: s.started, StartedAt: now, EndsAt: now.Add(dur), Status: "running"}, nil
}

func (s *fakeStore) FinishRun(_ context.Context, id int64, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finished[id] = status
	return nil
}

func (s *fakeStore) PurgeProcessed(context.Context, time.Time) (int64, error) { return 0, nil }

func (s *fakeStore) status(id int64) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finished[id]
}

type fakeEngine struct {
	errs   chan error
	acked  atomic.Int64
	waited atomic.Bool
}

func (e *fakeEngine) Start(context.Context) {}
func (e *fakeEngine) Kill(id int) error {
	if id == 99 {
		return scorer.ErrUnknownWorker
	}
	return nil
}
func (e *fakeEngine) Wait()                         { e.waited.Store(true) }
func (e *fakeEngine) Snapshot() []scorer.WorkerView { return []scorer.WorkerView{{ID: 0, Up: true, Partitions: []int{0}}} }
func (e *fakeEngine) Counters() scorer.Counters     { return scorer.Counters{Scored: 3} }
func (e *fakeEngine) Acked() []int64                { return []int64{e.acked.Load(), 0, 0, 0} }
func (e *fakeEngine) Errors() <-chan error          { return e.errs }

type fakeSource struct{ published int64 }

func (s *fakeSource) Run(ctx context.Context, _ int) error { <-ctx.Done(); return nil }
func (s *fakeSource) Inject(_ context.Context, name string) ([]domain.Tx, error) {
	if name == "nope" {
		return nil, gen.ErrUnknownScenario
	}
	return []domain.Tx{{CardID: "scn-1"}, {CardID: "scn-1"}}, nil
}
func (s *fakeSource) Published() []int64 { return []int64{s.published, 0, 0, 0} }

type recHub struct {
	mu     sync.Mutex
	events []hub.Event
}

func (h *recHub) Publish(e hub.Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.events = append(h.events, e)
}

func (h *recHub) count(typ string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, e := range h.events {
		if e.Type == typ {
			n++
		}
	}
	return n
}

type rig struct {
	c      *Controller
	st     *fakeStore
	engine *fakeEngine
	source *fakeSource
	hub    *recHub
	built  atomic.Int32
}

func newRig(t *testing.T, cfg Config) *rig {
	r := &rig{st: &fakeStore{finished: map[int64]string{}}, engine: &fakeEngine{errs: make(chan error, 1)},
		source: &fakeSource{}, hub: &recHub{}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r.c = New(ctx, cfg, r.st,
		func() Engine { r.built.Add(1); return r.engine },
		func() Source { return r.source }, r.hub, slog.New(slog.DiscardHandler))
	return r
}

func quick() Config {
	return Config{Duration: 100 * time.Millisecond, PerSecond: 5, DailyCap: 4, StatsEvery: 20 * time.Millisecond,
		Drain: 150 * time.Millisecond, Retention: time.Hour}
}

func waitStatus(t *testing.T, st *fakeStore, id int64, want string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for st.status(id) != want {
		if time.Now().After(deadline) {
			t.Fatalf("run %d status %q, want %q", id, st.status(id), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestRunStartsThenFinishes(t *testing.T) {
	r := newRig(t, quick())
	run, err := r.c.Start(t.Context())
	if err != nil || run.ID != 1 {
		t.Fatalf("start: %+v %v", run, err)
	}
	var active *store.ActiveError
	if _, err := r.c.Start(t.Context()); !errors.As(err, &active) {
		t.Fatalf("second start: %v", err)
	}
	if v := r.c.View(); v == nil || v.Status != "running" || len(v.Workers) != 1 {
		t.Fatalf("view during run: %+v", v)
	}
	waitStatus(t, r.st, 1, "finished")
	if !r.engine.waited.Load() || r.hub.count("run") != 2 || r.hub.count("stats") == 0 {
		t.Fatalf("waited=%v run events=%d stats=%d", r.engine.waited.Load(), r.hub.count("run"), r.hub.count("stats"))
	}
	if v := r.c.View(); v == nil || v.Status != "finished" {
		t.Fatalf("view after run: %+v", v)
	}
}

func TestStartPassesStoreErrorsThrough(t *testing.T) {
	r := newRig(t, quick())
	r.st.err = store.ErrDailyCap
	if _, err := r.c.Start(t.Context()); !errors.Is(err, store.ErrDailyCap) {
		t.Fatalf("got %v", err)
	}
	if r.built.Load() != 0 {
		t.Fatal("no engine may be built when the store refuses")
	}
}

func TestInjectAndKillAfterRunEnds(t *testing.T) {
	r := newRig(t, quick())
	if err := r.c.Inject(t.Context(), gen.CardTesting); !errors.Is(err, ErrNoRun) {
		t.Fatalf("inject before: %v", err)
	}
	if _, err := r.c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, r.st, 1, "finished")
	if err := r.c.Inject(t.Context(), gen.CardTesting); !errors.Is(err, ErrNoRun) {
		t.Fatalf("inject after: %v", err)
	}
	if err := r.c.Kill(0); !errors.Is(err, ErrNoRun) {
		t.Fatalf("kill after: %v", err)
	}
}

func TestInjectAndKillDuringARun(t *testing.T) {
	cfg := quick()
	cfg.Duration = time.Hour
	r := newRig(t, cfg)
	if _, err := r.c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := r.c.Inject(t.Context(), gen.CardTesting); err != nil || r.hub.count("scenario") != 1 {
		t.Fatalf("inject: %v, scenario events %d", err, r.hub.count("scenario"))
	}
	if err := r.c.Inject(t.Context(), "nope"); !errors.Is(err, gen.ErrUnknownScenario) {
		t.Fatalf("unknown scenario: %v", err)
	}
	if err := r.c.Kill(99); !errors.Is(err, scorer.ErrUnknownWorker) {
		t.Fatalf("kill passthrough: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	r.c.Shutdown(ctx)
	waitStatus(t, r.st, 1, "interrupted")
}

func TestEngineErrorEndsTheRun(t *testing.T) {
	cfg := quick()
	cfg.Duration = time.Hour
	r := newRig(t, cfg)
	if _, err := r.c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	r.engine.errs <- errors.New("redis down")
	waitStatus(t, r.st, 1, "error")
}

func TestDrainWaitsForLagAndReportsIt(t *testing.T) {
	r := newRig(t, quick())
	r.source.published = 5
	if _, err := r.c.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	if v := r.c.View(); v == nil || v.Lag[0] != 5 {
		t.Fatalf("lag: %+v", v)
	}
	began := time.Now()
	waitStatus(t, r.st, 1, "finished")
	if time.Since(began) < 150*time.Millisecond {
		t.Fatal("finish must wait out the drain while lag stays above zero")
	}
}
