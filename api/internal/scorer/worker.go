package scorer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/broker"
	"github.com/Kruthik481/streamguard/api/internal/domain"
	"github.com/Kruthik481/streamguard/api/internal/hub"
	"github.com/Kruthik481/streamguard/api/internal/rules"
)

const (
	noPartitionWait = 200 * time.Millisecond
	retryStart      = 100 * time.Millisecond
	retryCap        = 5 * time.Second
)

type Scored struct {
	Tx        domain.Tx    `json:"tx"`
	Worker    int          `json:"worker"`
	Partition int          `json:"partition"`
	Score     int          `json:"score"`
	Severity  string       `json:"severity"`
	Hits      []domain.Hit `json:"hits"`
}

type Duplicate struct {
	TxID      string `json:"tx_id"`
	Worker    int    `json:"worker"`
	Partition int    `json:"partition"`
}

type Redelivery struct {
	DeliveryID string `json:"delivery_id"`
	Worker     int    `json:"worker"`
	Partition  int    `json:"partition"`
}

type DeadLetter struct {
	DeliveryID string `json:"delivery_id"`
	Partition  int    `json:"partition"`
}

func (c *Coordinator) runWorker(ctx context.Context, id int, consumer string) {
	for ctx.Err() == nil {
		owned := c.owned(id)
		if len(owned) == 0 {
			sleep(ctx, noPartitionWait)
			continue
		}
		busy := false
		for _, p := range owned {
			n, err := c.servePartition(ctx, id, consumer, p)
			if err != nil {
				if ctx.Err() == nil {
					c.d.Log.Error("worker failed", "worker", id, "partition", p, "err", err)
					c.fail(err)
				}
				return
			}
			busy = busy || n >= c.cfg.Batch
		}
		if !busy {
			sleep(ctx, c.cfg.IdleWait) // short reads mean little traffic; back off to save Redis commands
		}
	}
}

func (c *Coordinator) servePartition(ctx context.Context, id int, consumer string, p int) (int, error) {
	c.locks[p].Lock()
	defer c.locks[p].Unlock()
	c.mu.Lock()
	mine, reclaim := c.owner[p] == id, c.needsReclaim[p]
	c.mu.Unlock()
	if !mine || ctx.Err() != nil {
		return 0, nil
	}
	if reclaim {
		return c.reclaim(ctx, id, consumer, p)
	}
	ds, err := c.d.Broker.Consume(ctx, p, consumer, c.cfg.Batch)
	if err != nil {
		return 0, err
	}
	return len(ds), c.handle(ctx, id, p, ds)
}

// reclaim drains other consumers' pending entries before the owner reads anything new, which
// keeps each partition in order across a rebalance.
func (c *Coordinator) reclaim(ctx context.Context, id int, consumer string, p int) (int, error) {
	ds, err := c.d.Broker.Reclaim(ctx, p, consumer, c.cfg.MinIdle)
	if err != nil {
		return 0, err
	}
	if err := c.handle(ctx, id, p, ds); err != nil {
		return 0, err
	}
	n, err := c.d.Broker.Pending(ctx, p, consumer)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		c.mu.Lock()
		if c.owner[p] == id {
			c.needsReclaim[p] = false
		}
		c.mu.Unlock()
	}
	return len(ds), nil
}

// handle processes a batch and acks it in one call. A worker killed mid-batch returns without
// acking, so everything it committed is redelivered and then deduplicated.
func (c *Coordinator) handle(ctx context.Context, id, p int, ds []broker.Delivery) error {
	done := make([]broker.Delivery, 0, len(ds))
	for _, d := range ds {
		if err := c.process(ctx, id, p, d); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		done = append(done, d)
	}
	if len(done) == 0 {
		return nil
	}
	if err := c.d.Broker.Ack(ctx, done...); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	c.acked[p].Add(int64(len(done)))
	return nil
}

func (c *Coordinator) process(ctx context.Context, id, p int, d broker.Delivery) error {
	if d.Redelivered {
		c.redelivered.Add(1)
		c.d.Hub.Publish(hub.Event{Type: "redelivered", Data: Redelivery{DeliveryID: d.ID, Worker: id, Partition: p}})
	}
	var tx domain.Tx
	if err := json.Unmarshal(d.Payload, &tx); err != nil || tx.ID == "" || tx.CardID == "" {
		if err := c.d.Broker.DeadLetter(ctx, d, "undecodable"); err != nil {
			return err
		}
		c.deadLettered.Add(1)
		c.d.Hub.Publish(hub.Event{Type: "dlq", Data: DeadLetter{DeliveryID: d.ID, Partition: p}})
		return nil
	}
	window, err := c.d.State.Apply(ctx, tx)
	if err != nil {
		return err
	}
	hits := rules.Evaluate(tx, window)
	inserted, err := c.recordWithRetry(ctx, tx, hits, domain.Meta{Partition: p, Worker: id})
	if err != nil {
		return err
	}
	if c.afterRecord != nil {
		c.afterRecord(id)
	}
	c.publishResult(id, p, tx, hits, inserted)
	return nil
}

// recordWithRetry keeps retrying a Postgres failure without acking, so messages wait in the
// stream (lag grows on the dashboard) instead of being lost.
func (c *Coordinator) recordWithRetry(ctx context.Context, tx domain.Tx, hits []domain.Hit, meta domain.Meta) (bool, error) {
	wait := retryStart
	for {
		// A kill must not abort a commit halfway: Postgres may commit while pgx reports the
		// cancellation, which would hide a scored tx. The ack check after this still sees the kill.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), recordTimeout)
		inserted, err := c.d.Store.Record(rctx, tx, hits, meta)
		cancel()
		if err == nil {
			return inserted, nil
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		c.d.Log.Warn("record failed, retrying", "tx", tx.ID, "err", err, "wait", wait)
		if !sleep(ctx, wait) {
			return false, ctx.Err()
		}
		wait = min(wait*2, retryCap)
	}
}

func (c *Coordinator) publishResult(id, p int, tx domain.Tx, hits []domain.Hit, inserted bool) {
	if !inserted {
		c.deduplicated.Add(1)
		c.d.Hub.Publish(hub.Event{Type: "deduplicated", Data: Duplicate{TxID: tx.ID, Worker: id, Partition: p}})
		return
	}
	c.scored.Add(1)
	c.perWorker[id].Add(1)
	if len(hits) > 0 {
		c.alerts.Add(1)
	}
	score := domain.Score(hits)
	c.d.Hub.Publish(hub.Event{Type: "scored", Data: Scored{Tx: tx, Worker: id, Partition: p,
		Score: score, Severity: domain.Severity(score), Hits: hits}})
}

// recordTimeout bounds one Record attempt, which runs outside the worker's cancellation.
var recordTimeout = 5 * time.Second

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
