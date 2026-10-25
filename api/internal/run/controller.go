// Package run owns the single shared demo run: start/limits, generator, scorer, stats, drain, end.
package run

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/domain"
	"github.com/Kruthik481/streamguard/api/internal/hub"
	"github.com/Kruthik481/streamguard/api/internal/scorer"
	"github.com/Kruthik481/streamguard/api/internal/store"
)

var ErrNoRun = errors.New("no run is active")

type RunStore interface {
	StartRun(ctx context.Context, now time.Time, dur time.Duration, dailyCap int) (store.Run, error)
	FinishRun(ctx context.Context, id int64, status string) error
	PurgeProcessed(ctx context.Context, before time.Time) (int64, error)
}

type Engine interface {
	Start(ctx context.Context)
	Kill(id int) error
	Wait()
	Snapshot() []scorer.WorkerView
	Counters() scorer.Counters
	Acked() []int64
	Errors() <-chan error
}

type Source interface {
	Run(ctx context.Context, perSecond int) error
	Inject(ctx context.Context, name string) ([]domain.Tx, error)
	Published() []int64
}

type Publisher interface{ Publish(hub.Event) }

type Config struct {
	Duration                     time.Duration
	PerSecond, DailyCap          int
	StatsEvery, Drain, Retention time.Duration
}

func DefaultConfig() Config {
	return Config{Duration: 180 * time.Second, PerSecond: 5, DailyCap: 4, StatsEvery: 2 * time.Second,
		Drain: 10 * time.Second, Retention: 7 * 24 * time.Hour}
}

type View struct {
	ID        int64               `json:"id"`
	Status    string              `json:"status"`
	StartedAt time.Time           `json:"started_at"`
	EndsAt    time.Time           `json:"ends_at"`
	Workers   []scorer.WorkerView `json:"workers"`
	Counters  scorer.Counters     `json:"counters"`
	Lag       []int64             `json:"lag"`
}

type ScenarioView struct {
	Name  string   `json:"name"`
	Cards []string `json:"cards"`
}

type active struct {
	run        store.Run
	engine     Engine
	source     Source
	engCtx     context.Context
	genCtx     context.Context
	stopEngine context.CancelFunc
	stopGen    context.CancelFunc
	done       chan struct{}
}

type Controller struct {
	cfg       Config
	st        RunStore
	newEngine func() Engine
	newSource func() Source
	pub       Publisher
	log       *slog.Logger
	base      context.Context

	mu   sync.Mutex
	cur  *active
	last *View
}

func New(base context.Context, cfg Config, st RunStore, newEngine func() Engine, newSource func() Source, pub Publisher, log *slog.Logger) *Controller {
	return &Controller{cfg: cfg, st: st, newEngine: newEngine, newSource: newSource, pub: pub, log: log, base: base}
}

func (c *Controller) Start(ctx context.Context) (store.Run, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cur != nil {
		return store.Run{}, &store.ActiveError{EndsAt: c.cur.run.EndsAt}
	}
	now := time.Now()
	if n, err := c.st.PurgeProcessed(ctx, now.Add(-c.cfg.Retention)); err != nil {
		c.log.Warn("purge failed", "err", err)
	} else if n > 0 {
		c.log.Info("purged processed rows", "rows", n)
	}
	r, err := c.st.StartRun(ctx, now, c.cfg.Duration, c.cfg.DailyCap)
	if err != nil {
		return store.Run{}, err
	}
	engCtx, stopEngine := context.WithCancel(c.base)
	genCtx, stopGen := context.WithDeadline(engCtx, r.EndsAt)
	a := &active{run: r, engine: c.newEngine(), source: c.newSource(), engCtx: engCtx, genCtx: genCtx,
		stopEngine: stopEngine, stopGen: stopGen, done: make(chan struct{})}
	a.engine.Start(engCtx)
	c.cur = a
	go c.supervise(a)
	c.pub.Publish(hub.Event{Type: "run", Data: c.viewOf(a, r.Status)})
	return r, nil
}

func (c *Controller) supervise(a *active) {
	defer close(a.done)
	genDone := make(chan error, 1)
	go func() { genDone <- a.source.Run(a.genCtx, c.cfg.PerSecond) }()
	ticker := time.NewTicker(c.cfg.StatsEvery)
	defer ticker.Stop()
	status := "finished"
loop:
	for {
		select {
		case err := <-genDone:
			if err != nil {
				status = "error"
				c.log.Error("generator failed", "err", err)
			}
			break loop
		case err := <-a.engine.Errors():
			status = "error"
			c.log.Error("scorer failed", "err", err)
			break loop
		case <-ticker.C:
			c.pub.Publish(hub.Event{Type: "stats", Data: c.viewOf(a, "running")})
		}
	}
	a.stopGen()
	if a.engCtx.Err() != nil {
		status = "interrupted"
	}
	if status == "finished" {
		c.drain(a)
	}
	a.stopEngine()
	a.engine.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.st.FinishRun(ctx, a.run.ID, status); err != nil {
		c.log.Error("finish run failed", "run", a.run.ID, "err", err)
	}
	final := c.viewOf(a, status)
	c.mu.Lock()
	c.cur, c.last = nil, &final
	c.mu.Unlock()
	c.pub.Publish(hub.Event{Type: "run", Data: final})
}

// drain lets workers finish what the generator already published, up to cfg.Drain.
func (c *Controller) drain(a *active) {
	deadline := time.Now().Add(c.cfg.Drain)
	for time.Now().Before(deadline) && a.engCtx.Err() == nil {
		total := int64(0)
		for _, n := range lag(a) {
			total += n
		}
		if total == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func lag(a *active) []int64 {
	published, acked := a.source.Published(), a.engine.Acked()
	out := make([]int64, len(published))
	for p := range published {
		out[p] = max(0, published[p]-acked[p])
	}
	return out
}

func (c *Controller) viewOf(a *active, status string) View {
	return View{ID: a.run.ID, Status: status, StartedAt: a.run.StartedAt, EndsAt: a.run.EndsAt,
		Workers: a.engine.Snapshot(), Counters: a.engine.Counters(), Lag: lag(a)}
}

func (c *Controller) current() *active {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cur
}

func (c *Controller) Inject(ctx context.Context, name string) error {
	a := c.current()
	if a == nil || a.genCtx.Err() != nil {
		return ErrNoRun
	}
	txs, err := a.source.Inject(ctx, name)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	var cards []string
	for _, tx := range txs {
		if !seen[tx.CardID] {
			seen[tx.CardID] = true
			cards = append(cards, tx.CardID)
		}
	}
	c.pub.Publish(hub.Event{Type: "scenario", Data: ScenarioView{Name: name, Cards: cards}})
	return nil
}

func (c *Controller) Kill(id int) error {
	a := c.current()
	if a == nil || a.engCtx.Err() != nil {
		return ErrNoRun
	}
	return a.engine.Kill(id)
}

// View is the running run, else the last finished one, else nil.
func (c *Controller) View() *View {
	c.mu.Lock()
	a, last := c.cur, c.last
	c.mu.Unlock()
	if a != nil {
		v := c.viewOf(a, "running")
		return &v
	}
	return last
}

// Shutdown stops the current run (status "interrupted") and waits for it to wind down.
func (c *Controller) Shutdown(ctx context.Context) {
	a := c.current()
	if a == nil {
		return
	}
	a.stopEngine()
	select {
	case <-a.done:
	case <-ctx.Done():
	}
}
