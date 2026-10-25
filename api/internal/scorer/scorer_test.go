package scorer

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kruthik481/streamguard/api/internal/broker"
	"github.com/Kruthik481/streamguard/api/internal/broker/redisstreams"
	"github.com/Kruthik481/streamguard/api/internal/domain"
	"github.com/Kruthik481/streamguard/api/internal/gen"
	"github.com/Kruthik481/streamguard/api/internal/hub"
	"github.com/Kruthik481/streamguard/api/internal/rules"
	"github.com/Kruthik481/streamguard/api/internal/state"
	"github.com/Kruthik481/streamguard/api/internal/store"
	"github.com/Kruthik481/streamguard/api/internal/testutil"
)

type harness struct {
	c    *Coordinator
	b    broker.Broker
	pool *pgxpool.Pool
	gen  *gen.Generator
}

func testConfig() Config {
	return Config{Workers: 4, Partitions: 4, Batch: 10, MinIdle: 200 * time.Millisecond, RestartAfter: time.Hour, IdleWait: 10 * time.Millisecond}
}

func newHarness(t *testing.T, cfg Config) *harness {
	t.Helper()
	rdb, prefix := testutil.Redis(t)
	pool := testutil.Postgres(t, store.Migrate)
	b := redisstreams.New(rdb, prefix, cfg.Partitions)
	if err := b.Setup(t.Context()); err != nil {
		t.Fatal(err)
	}
	c := New(cfg, Deps{Broker: b, State: state.New(rdb, prefix), Store: store.New(pool),
		Hub: hub.New(1 << 14), Log: slog.New(slog.DiscardHandler)})
	return &harness{c: c, b: b, pool: pool, gen: gen.New(b, cfg.Partitions, 42, time.Now)}
}

func (h *harness) start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	h.c.Start(ctx)
	t.Cleanup(func() { cancel(); h.c.Wait() })
}

func (h *harness) publish(t *testing.T, txs ...domain.Tx) {
	for _, tx := range txs {
		if err := h.gen.Publish(t.Context(), tx); err != nil {
			t.Fatal(err)
		}
	}
}

func (h *harness) pending(t *testing.T) int64 {
	var total int64
	for p := range 4 {
		n, err := h.b.Pending(t.Context(), p, "")
		if err != nil {
			t.Fatal(err)
		}
		total += n
	}
	return total
}

func (h *harness) alertTxIDs(t *testing.T) []string {
	rows, err := h.pool.Query(t.Context(), `SELECT tx_id FROM alerts ORDER BY tx_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		ids = append(ids, id)
	}
	return ids
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// expectedAlerts replays txs card by card in publish order with the same window limits as state.
func expectedAlerts(txs []domain.Tx) []string {
	byCard := map[string][]domain.Tx{}
	var ids []string
	for _, tx := range txs {
		w := append(byCard[tx.CardID], tx)
		sort.Slice(w, func(i, j int) bool { return w[i].Before(w[j]) })
		if len(w) > state.MaxEntries {
			w = w[len(w)-state.MaxEntries:]
		}
		byCard[tx.CardID] = w
		if len(rules.Evaluate(tx, w)) > 0 {
			ids = append(ids, tx.ID)
		}
	}
	slices.Sort(ids)
	return ids
}

func TestScoresEveryTransactionOnceAndAcks(t *testing.T) {
	h := newHarness(t, testConfig())
	var txs []domain.Tx
	for range 40 {
		txs = append(txs, h.gen.Normal())
	}
	h.publish(t, txs...)
	h.start(t)
	waitFor(t, 10*time.Second, "all scored and acked", func() bool {
		return h.c.Counters().Scored == 40 && h.pending(t) == 0
	})
	var acked int64
	for _, n := range h.c.Acked() {
		acked += n
	}
	if acked != 40 {
		t.Fatalf("acked %d", acked)
	}
}

func TestCrashAfterCommitIsDeduplicated(t *testing.T) {
	h := newHarness(t, testConfig())
	txs, err := h.gen.Scenario(gen.CardTesting)
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	h.c.afterRecord = func(worker int) { once.Do(func() { _ = h.c.Kill(worker) }) }
	h.publish(t, txs...)
	h.start(t)
	waitFor(t, 10*time.Second, "scenario fully scored", func() bool {
		return h.c.Counters().Scored == int64(len(txs)) && h.pending(t) == 0
	})
	c := h.c.Counters()
	if c.Deduplicated < 1 || c.Redelivered < 1 {
		t.Fatalf("expected a redelivery and a suppressed duplicate: %+v", c)
	}
	if got, want := h.alertTxIDs(t), expectedAlerts(txs); !slices.Equal(got, want) {
		t.Fatalf("alerts %v, want %v", got, want)
	}
}

func TestRandomKillsMatchASingleWorkerReference(t *testing.T) {
	cfg := testConfig()
	cfg.RestartAfter = 300 * time.Millisecond
	h := newHarness(t, cfg)
	var all []domain.Tx
	for i := range 1000 {
		all = append(all, h.gen.Normal())
		if i%100 == 0 {
			sc, _ := h.gen.Scenario(gen.Scenarios[(i/100)%len(gen.Scenarios)])
			all = append(all, sc...)
		}
	}
	h.publish(t, all...)
	h.start(t)
	stop := make(chan struct{})
	go func() {
		r := rand.New(rand.NewPCG(1, 2))
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				_ = h.c.Kill(r.IntN(4))
			}
		}
	}()
	waitFor(t, 60*time.Second, "every transaction scored", func() bool {
		return h.c.Counters().Scored == int64(len(all)) && h.pending(t) == 0
	})
	close(stop)
	if got, want := h.alertTxIDs(t), expectedAlerts(all); !slices.Equal(got, want) {
		t.Fatalf("alert set differs: got %d want %d", len(got), len(want))
	}
}

func TestKillGuards(t *testing.T) {
	h := newHarness(t, testConfig())
	h.start(t)
	if err := h.c.Kill(9); err != ErrUnknownWorker {
		t.Fatalf("unknown: %v", err)
	}
	if err := h.c.Kill(0); err != nil {
		t.Fatal(err)
	}
	if err := h.c.Kill(0); err != ErrWorkerDown {
		t.Fatalf("twice: %v", err)
	}
	_ = h.c.Kill(1)
	_ = h.c.Kill(2)
	if err := h.c.Kill(3); err != ErrLastWorker {
		t.Fatalf("last: %v", err)
	}
	for _, w := range h.c.Snapshot() {
		if w.ID == 3 && len(w.Partitions) != 4 {
			t.Fatalf("worker 3 must own every partition: %+v", w)
		}
	}
}

func TestKilledWorkerRestartsAndTakesItsPartitionBack(t *testing.T) {
	cfg := testConfig()
	cfg.RestartAfter = 100 * time.Millisecond
	h := newHarness(t, cfg)
	h.start(t)
	if err := h.c.Kill(1); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 3*time.Second, "worker 1 back on partition 1", func() bool {
		for _, w := range h.c.Snapshot() {
			if w.ID == 1 {
				return w.Up && slices.Equal(w.Partitions, []int{1})
			}
		}
		return false
	})
}

func TestPoisonMessageIsDeadLettered(t *testing.T) {
	h := newHarness(t, testConfig())
	if err := h.b.Publish(t.Context(), 0, broker.Message{Key: "x", Payload: []byte("{not json")}); err != nil {
		t.Fatal(err)
	}
	h.start(t)
	waitFor(t, 5*time.Second, "dead letter", func() bool {
		return h.c.Counters().DeadLettered == 1 && h.pending(t) == 0
	})
}

func TestLeftoverPendingIsReclaimedOnStart(t *testing.T) {
	h := newHarness(t, testConfig())
	tx := h.gen.Normal()
	h.publish(t, tx)
	p := gen.PartitionFor(tx.CardID, 4)
	if ds, err := h.b.Consume(t.Context(), p, "crashed-process", 10); err != nil || len(ds) != 1 {
		t.Fatalf("setup consume: %d %v", len(ds), err)
	}
	h.start(t)
	waitFor(t, 5*time.Second, "leftover reclaimed", func() bool {
		return h.c.Counters().Scored == 1 && h.pending(t) == 0
	})
	if h.c.Counters().Redelivered != 1 {
		t.Fatalf("counters %+v", h.c.Counters())
	}
}
