// Package state keeps each card's recent transactions in a Redis sorted set.
// Writes are idempotent: the member is the transaction's canonical JSON, so a redelivered
// transaction re-adds the same member and changes nothing.
package state

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Kruthik481/streamguard/api/internal/domain"
)

const (
	MaxEntries = 30
	Span       = 24 * time.Hour
)

//go:embed window.lua
var windowLua string

var script = redis.NewScript(windowLua)

type Windows struct {
	rdb    redis.UniversalClient
	prefix string
}

func New(rdb redis.UniversalClient, prefix string) *Windows {
	return &Windows{rdb: rdb, prefix: prefix}
}

// Apply adds tx to its card's window (one EVALSHA) and returns the window in event-time order.
func (w *Windows) Apply(ctx context.Context, tx domain.Tx) ([]domain.Tx, error) {
	tx.EventTime = tx.EventTime.UTC()
	member, err := json.Marshal(tx)
	if err != nil {
		return nil, err
	}
	ms := tx.EventTime.UnixMilli()
	raw, err := script.Run(ctx, w.rdb, []string{w.prefix + "card:" + tx.CardID},
		ms, member, ms-Span.Milliseconds(), MaxEntries, Span.Milliseconds()).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("apply window for %s: %w", tx.CardID, err)
	}
	out := make([]domain.Tx, 0, len(raw))
	for _, s := range raw {
		var e domain.Tx
		if err := json.Unmarshal([]byte(s), &e); err != nil {
			return nil, fmt.Errorf("decode window entry: %w", err)
		}
		out = append(out, e)
	}
	return out, nil
}
