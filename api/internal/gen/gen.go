// Package gen produces synthetic card transactions and injects attack scenarios.
package gen

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/Kruthik481/streamguard/api/internal/broker"
	"github.com/Kruthik481/streamguard/api/internal/domain"
	"github.com/Kruthik481/streamguard/api/internal/rules"
)

const (
	cardCount        = 200
	merchantsPerCard = 8
	merchantPool     = 1000
	minTypicalCents  = 500
	typicalSpread    = 11500 // typical amounts span $5–$120
)

type Publisher interface {
	Publish(ctx context.Context, partition int, msg broker.Message) error
}

type card struct {
	id        string
	country   string
	merchants []string
	typical   int64
}

type Generator struct {
	pub        Publisher
	partitions int
	now        func() time.Time

	mu    sync.Mutex
	rng   *rand.Rand
	cards []card

	published []atomic.Int64
}

func New(pub Publisher, partitions int, seed int64, now func() time.Time) *Generator {
	rng := rand.New(rand.NewPCG(uint64(seed), 0x5eed))
	countries := rules.Countries()
	cards := make([]card, cardCount)
	for i := range cards {
		merchants := make([]string, merchantsPerCard)
		for j := range merchants {
			merchants[j] = fmt.Sprintf("m-%04d", rng.IntN(merchantPool))
		}
		cards[i] = card{
			id:        fmt.Sprintf("card-%03d", i),
			country:   countries[rng.IntN(len(countries))],
			merchants: merchants,
			typical:   minTypicalCents + rng.Int64N(typicalSpread),
		}
	}
	return &Generator{pub: pub, partitions: partitions, now: now, rng: rng, cards: cards,
		published: make([]atomic.Int64, partitions)}
}

func PartitionFor(cardID string, partitions int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(cardID))
	return int(h.Sum32() % uint32(partitions))
}

func (g *Generator) stamp(offset time.Duration) time.Time {
	return g.now().UTC().Add(offset).Truncate(time.Millisecond)
}

// Normal is one ordinary purchase: home country, a usual merchant, 0.5–1.5× the card's typical amount.
func (g *Generator) Normal() domain.Tx {
	g.mu.Lock()
	defer g.mu.Unlock()
	c := g.cards[g.rng.IntN(len(g.cards))]
	return domain.Tx{
		ID:          ulid.Make().String(),
		CardID:      c.id,
		AmountCents: int64(float64(c.typical) * (0.5 + g.rng.Float64())),
		MerchantID:  c.merchants[g.rng.IntN(len(c.merchants))],
		Country:     c.country,
		EventTime:   g.stamp(0),
	}
}

func (g *Generator) Publish(ctx context.Context, tx domain.Tx) error {
	payload, err := json.Marshal(tx)
	if err != nil {
		return err
	}
	p := PartitionFor(tx.CardID, g.partitions)
	if err := g.pub.Publish(ctx, p, broker.Message{Key: tx.CardID, Payload: payload}); err != nil {
		return err
	}
	g.published[p].Add(1)
	return nil
}

// Run publishes normal traffic at perSecond until ctx ends, then returns nil.
func (g *Generator) Run(ctx context.Context, perSecond int) error {
	ticker := time.NewTicker(time.Second / time.Duration(perSecond))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := g.Publish(ctx, g.Normal()); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}
	}
}

func (g *Generator) Inject(ctx context.Context, name string) ([]domain.Tx, error) {
	txs, err := g.Scenario(name)
	if err != nil {
		return nil, err
	}
	for _, tx := range txs {
		if err := g.Publish(ctx, tx); err != nil {
			return nil, err
		}
	}
	return txs, nil
}

func (g *Generator) Published() []int64 {
	out := make([]int64, len(g.published))
	for i := range g.published {
		out[i] = g.published[i].Load()
	}
	return out
}
