// Package scorer runs the consumer-group workers: exclusive partition ownership, reclaim on
// rebalance, idempotent processing and the kill/restart demo.
package scorer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/Kruthik481/streamguard/api/internal/broker"
	"github.com/Kruthik481/streamguard/api/internal/domain"
	"github.com/Kruthik481/streamguard/api/internal/hub"
)

var (
	ErrUnknownWorker = errors.New("unknown worker")
	ErrWorkerDown    = errors.New("worker is already down")
	ErrLastWorker    = errors.New("cannot kill the last live worker")
)

type Applier interface {
	Apply(ctx context.Context, tx domain.Tx) ([]domain.Tx, error)
}

type Recorder interface {
	Record(ctx context.Context, tx domain.Tx, hits []domain.Hit, meta domain.Meta) (bool, error)
}

type Publisher interface{ Publish(e hub.Event) }

type Config struct {
	Workers, Partitions, Batch      int
	MinIdle, RestartAfter, IdleWait time.Duration
}

func DefaultConfig() Config {
	return Config{Workers: 4, Partitions: 4, Batch: 10, MinIdle: 5 * time.Second, RestartAfter: 15 * time.Second, IdleWait: time.Second}
}

type Deps struct {
	Broker broker.Broker
	State  Applier
	Store  Recorder
	Hub    Publisher
	Log    *slog.Logger
}

type WorkerView struct {
	ID         int   `json:"id"`
	Up         bool  `json:"up"`
	Partitions []int `json:"partitions"`
	Scored     int64 `json:"scored"`
}

type Counters struct {
	Scored       int64 `json:"scored"`
	Alerts       int64 `json:"alerts"`
	Deduplicated int64 `json:"deduplicated"`
	Redelivered  int64 `json:"redelivered"`
	DeadLettered int64 `json:"dead_lettered"`
}

type Move struct {
	Partition int `json:"partition"`
	From      int `json:"from"`
	To        int `json:"to"`
}

type WorkerChange struct {
	ID int  `json:"id"`
	Up bool `json:"up"`
}

type Coordinator struct {
	cfg   Config
	d     Deps
	token string

	mu           sync.Mutex
	runCtx       context.Context
	stopping     bool
	owner        []int
	needsReclaim []bool
	up           []bool
	generation   []int
	cancel       []context.CancelFunc

	locks []sync.Mutex
	wg    sync.WaitGroup
	fatal chan error

	scored, alerts, deduplicated, redelivered, deadLettered atomic.Int64
	perWorker                                               []atomic.Int64
	acked                                                   []atomic.Int64

	afterRecord func(worker int) // test hook: after the Postgres commit, before the ack
}

func New(cfg Config, d Deps) *Coordinator {
	return &Coordinator{
		cfg: cfg, d: d, token: strings.ToLower(ulid.Make().String()[20:]),
		owner: make([]int, cfg.Partitions), needsReclaim: make([]bool, cfg.Partitions),
		up: make([]bool, cfg.Workers), generation: make([]int, cfg.Workers), cancel: make([]context.CancelFunc, cfg.Workers),
		locks: make([]sync.Mutex, cfg.Partitions), fatal: make(chan error, 1),
		perWorker: make([]atomic.Int64, cfg.Workers), acked: make([]atomic.Int64, cfg.Partitions),
	}
}

func (c *Coordinator) home(p int) int { return p % c.cfg.Workers }

// Start assigns every partition to its home worker, flags all for reclaim (so leftovers from a
// previous process are recovered first) and starts the workers.
func (c *Coordinator) Start(ctx context.Context) {
	c.mu.Lock()
	c.runCtx = ctx
	for p := range c.cfg.Partitions {
		c.owner[p] = c.home(p)
		c.needsReclaim[p] = true
	}
	c.mu.Unlock()
	for id := range c.cfg.Workers {
		c.startWorker(id)
	}
}

func (c *Coordinator) startWorker(id int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopping || c.runCtx.Err() != nil {
		return false
	}
	c.generation[id]++
	c.up[id] = true
	wctx, cancel := context.WithCancel(c.runCtx)
	c.cancel[id] = cancel
	consumer := fmt.Sprintf("w%d-%s-g%d", id, c.token, c.generation[id])
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.runWorker(wctx, id, consumer)
	}()
	return true
}

// Kill cancels a worker without acking its in-flight messages and hands its partitions to the
// live workers round-robin. The worker restarts after RestartAfter and takes its home back.
func (c *Coordinator) Kill(id int) error {
	c.mu.Lock()
	if id < 0 || id >= c.cfg.Workers {
		c.mu.Unlock()
		return ErrUnknownWorker
	}
	if !c.up[id] {
		c.mu.Unlock()
		return ErrWorkerDown
	}
	var live []int
	for w := range c.cfg.Workers {
		if w != id && c.up[w] {
			live = append(live, w)
		}
	}
	if len(live) == 0 {
		c.mu.Unlock()
		return ErrLastWorker
	}
	c.up[id] = false
	c.cancel[id]()
	var moves []Move
	for p, o := range c.owner {
		if o == id {
			to := live[len(moves)%len(live)]
			c.owner[p] = to
			c.needsReclaim[p] = true
			moves = append(moves, Move{Partition: p, From: id, To: to})
		}
	}
	c.mu.Unlock()
	c.d.Hub.Publish(hub.Event{Type: "worker", Data: WorkerChange{ID: id, Up: false}})
	if len(moves) > 0 { // a nil slice would reach the dashboard as JSON null
		c.d.Hub.Publish(hub.Event{Type: "rebalance", Data: moves})
	}
	time.AfterFunc(c.cfg.RestartAfter, func() { c.restart(id) })
	return nil
}

func (c *Coordinator) restart(id int) {
	if !c.startWorker(id) {
		return
	}
	c.mu.Lock()
	var moves []Move
	for p := range c.owner {
		if c.home(p) == id && c.owner[p] != id {
			moves = append(moves, Move{Partition: p, From: c.owner[p], To: id})
			c.owner[p] = id
			c.needsReclaim[p] = true
		}
	}
	c.mu.Unlock()
	c.d.Hub.Publish(hub.Event{Type: "worker", Data: WorkerChange{ID: id, Up: true}})
	if len(moves) > 0 {
		c.d.Hub.Publish(hub.Event{Type: "rebalance", Data: moves})
	}
}

// Wait blocks until every worker has exited; call it after cancelling Start's context.
func (c *Coordinator) Wait() {
	c.mu.Lock()
	c.stopping = true
	c.mu.Unlock()
	c.wg.Wait()
}

func (c *Coordinator) Errors() <-chan error { return c.fatal }

func (c *Coordinator) fail(err error) {
	select {
	case c.fatal <- err:
	default:
	}
}

func (c *Coordinator) owned(id int) []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var ps []int
	for p, o := range c.owner {
		if o == id {
			ps = append(ps, p)
		}
	}
	return ps
}

func (c *Coordinator) Snapshot() []WorkerView {
	c.mu.Lock()
	defer c.mu.Unlock()
	views := make([]WorkerView, c.cfg.Workers)
	for id := range views {
		views[id] = WorkerView{ID: id, Up: c.up[id], Partitions: []int{}, Scored: c.perWorker[id].Load()}
	}
	for p, o := range c.owner {
		views[o].Partitions = append(views[o].Partitions, p)
	}
	return views
}

func (c *Coordinator) Counters() Counters {
	return Counters{Scored: c.scored.Load(), Alerts: c.alerts.Load(), Deduplicated: c.deduplicated.Load(),
		Redelivered: c.redelivered.Load(), DeadLettered: c.deadLettered.Load()}
}

func (c *Coordinator) Acked() []int64 {
	out := make([]int64, len(c.acked))
	for p := range c.acked {
		out[p] = c.acked[p].Load()
	}
	return out
}
