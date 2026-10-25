package gen

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/broker"
	"github.com/Kruthik481/streamguard/api/internal/domain"
	"github.com/Kruthik481/streamguard/api/internal/rules"
)

type sink struct {
	mu  sync.Mutex
	got []broker.Message
	ps  []int
}

func (s *sink) Publish(_ context.Context, p int, m broker.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, m)
	s.ps = append(s.ps, p)
	return nil
}

func (s *sink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

func TestPartitionForIsStableAndInRange(t *testing.T) {
	for _, card := range []string{"card-001", "card-199", "scn-abc"} {
		p := PartitionFor(card, 4)
		if p < 0 || p >= 4 || p != PartitionFor(card, 4) {
			t.Fatalf("%s -> %d", card, p)
		}
	}
}

func TestNormalTransactionsAreQuiet(t *testing.T) {
	g := New(&sink{}, 4, 7, time.Now)
	byCard := map[string][]domain.Tx{}
	for range 2000 {
		tx := g.Normal()
		if tx.ID == "" || tx.AmountCents < 250 || tx.AmountCents > 18000 || !slices.Contains(rules.Countries(), tx.Country) {
			t.Fatalf("bad tx %+v", tx)
		}
		// Space each card's purchases an hour apart so velocity (the only rule ordinary traffic can
		// trip, by chance) is out of the picture; every other rule must stay silent.
		tx.EventTime = tx.EventTime.Add(time.Duration(len(byCard[tx.CardID])) * time.Hour)
		byCard[tx.CardID] = append(byCard[tx.CardID], tx)
	}
	for card, txs := range byCard {
		for i, tx := range txs {
			if hits := rules.Evaluate(tx, txs[:i+1]); len(hits) > 0 {
				t.Fatalf("normal traffic on %s fired %+v", card, hits)
			}
		}
	}
}

func lastHits(txs []domain.Tx) []string {
	var names []string
	for _, h := range rules.Evaluate(txs[len(txs)-1], txs) {
		names = append(names, h.Rule)
	}
	return names
}

func TestScenariosTriggerTheirRules(t *testing.T) {
	g := New(&sink{}, 4, 7, time.Now)
	cases := map[string][]string{
		CardTesting:      {rules.CardTesting, rules.Velocity},
		ImpossibleTravel: {rules.ImpossibleTravel},
		AccountTakeover:  {rules.AmountSpike, rules.NewMerchantHighAmount},
	}
	for name, want := range cases {
		t.Run(name, func(t *testing.T) {
			txs, err := g.Scenario(name)
			if err != nil {
				t.Fatal(err)
			}
			card := txs[0].CardID
			for _, tx := range txs {
				if tx.CardID != card {
					t.Fatal("a scenario stays on one card")
				}
			}
			got := lastHits(txs)
			for _, w := range want {
				if !slices.Contains(got, w) {
					t.Fatalf("missing %s in %v", w, got)
				}
			}
		})
	}
	if _, err := g.Scenario("nope"); !errors.Is(err, ErrUnknownScenario) {
		t.Fatalf("want ErrUnknownScenario, got %v", err)
	}
}

func TestInjectPublishesToTheCardsPartition(t *testing.T) {
	s := &sink{}
	g := New(s, 4, 7, time.Now)
	txs, err := g.Inject(t.Context(), CardTesting)
	if err != nil || s.count() != len(txs) {
		t.Fatalf("published %d of %d, %v", s.count(), len(txs), err)
	}
	want := PartitionFor(txs[0].CardID, 4)
	for i, p := range s.ps {
		var decoded domain.Tx
		if err := json.Unmarshal(s.got[i].Payload, &decoded); err != nil || p != want || s.got[i].Key != txs[0].CardID {
			t.Fatalf("message %d: p=%d key=%s err=%v", i, p, s.got[i].Key, err)
		}
	}
	if g.Published()[want] != int64(len(txs)) {
		t.Fatalf("published counter %v", g.Published())
	}
}

func TestRunPublishesAtTheRequestedRate(t *testing.T) {
	s := &sink{}
	g := New(s, 4, 7, time.Now)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if err := g.Run(ctx, 100); err != nil {
		t.Fatal(err)
	}
	if n := s.count(); n < 20 || n > 35 {
		t.Fatalf("published %d in 300ms at 100/s", n)
	}
}
