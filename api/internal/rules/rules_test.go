package rules

import (
	"math"
	"slices"
	"testing"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/domain"
)

var t0 = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

func tx(id string, at time.Duration, cents int64, merchant, country string) domain.Tx {
	return domain.Tx{ID: id, CardID: "c1", AmountCents: cents, MerchantID: merchant, Country: country, EventTime: t0.Add(at)}
}

// series builds same-merchant, same-country, $10 transactions at the given offsets.
func series(offsets ...time.Duration) []domain.Tx {
	out := make([]domain.Tx, len(offsets))
	for i, at := range offsets {
		out[i] = tx(string(rune('a'+i)), at, 1000, "m1", "US")
	}
	return out
}

func fired(t *testing.T, window []domain.Tx, current domain.Tx) []string {
	t.Helper()
	var names []string
	for _, h := range Evaluate(current, window) {
		names = append(names, h.Rule)
	}
	return names
}

func last(w []domain.Tx) domain.Tx { return w[len(w)-1] }

func TestVelocity(t *testing.T) {
	s := time.Second
	cases := []struct {
		name string
		w    []domain.Tx
		want bool
	}{
		{"five within exactly 60s fires", series(0, 10*s, 20*s, 30*s, 60*s), true},
		{"four does not", series(0, 10*s, 20*s, 30*s), false},
		{"first falls just outside the window", series(0, 10*s, 20*s, 30*s, 60*s+time.Millisecond), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := slices.Contains(fired(t, c.w, last(c.w)), Velocity); got != c.want {
				t.Fatalf("velocity fired=%v want %v", got, c.want)
			}
		})
	}
}

func TestCardTesting(t *testing.T) {
	s := time.Second
	small := func(id string, at time.Duration) domain.Tx { return tx(id, at, 150, "m1", "US") }
	cases := []struct {
		name string
		w    []domain.Tx
		want bool
	}{
		{"three small within 120s fires", []domain.Tx{small("a", 0), small("b", 60*s), small("c", 120*s)}, true},
		{"two small does not", []domain.Tx{small("a", 0), small("b", 60*s)}, false},
		{"spread over more than 120s does not", []domain.Tx{small("a", 0), small("b", 60*s), small("c", 121*s)}, false},
		{"current not small does not", []domain.Tx{small("a", 0), small("b", 10*s), tx("c", 20*s, 200, "m1", "US")}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := slices.Contains(fired(t, c.w, last(c.w)), CardTesting); got != c.want {
				t.Fatalf("card_testing fired=%v want %v", got, c.want)
			}
		})
	}
}

func TestImpossibleTravel(t *testing.T) {
	km := distanceKm("IN", "US")
	atLimit := time.Duration(km / MaxSpeedKmh * float64(time.Hour))
	cases := []struct {
		name string
		w    []domain.Tx
		want bool
	}{
		{"India then US ten minutes later fires", []domain.Tx{tx("a", 0, 1000, "m1", "IN"), tx("b", 10*time.Minute, 1000, "m2", "US")}, true},
		{"same instant in two countries fires", []domain.Tx{tx("a", 0, 1000, "m1", "IN"), tx("b", 0, 1000, "m2", "US")}, true},
		{"slightly slower than the limit does not", []domain.Tx{tx("a", 0, 1000, "m1", "IN"), tx("b", atLimit+time.Second, 1000, "m2", "US")}, false},
		{"slightly faster than the limit fires", []domain.Tx{tx("a", 0, 1000, "m1", "IN"), tx("b", atLimit-time.Second, 1000, "m2", "US")}, true},
		{"same country does not", []domain.Tx{tx("a", 0, 1000, "m1", "US"), tx("b", time.Minute, 1000, "m2", "US")}, false},
		{"unknown country does not", []domain.Tx{tx("a", 0, 1000, "m1", "ZZ"), tx("b", time.Minute, 1000, "m2", "US")}, false},
		{"first transaction does not", []domain.Tx{tx("a", 0, 1000, "m1", "US")}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := slices.Contains(fired(t, c.w, last(c.w)), ImpossibleTravel); got != c.want {
				t.Fatalf("impossible_travel fired=%v want %v", got, c.want)
			}
		})
	}
}

func TestAmountSpike(t *testing.T) {
	h := time.Hour
	history := func(n int) []domain.Tx {
		var w []domain.Tx
		for i := range n {
			w = append(w, tx(string(rune('a'+i)), time.Duration(i)*h, 1000, "m1", "US"))
		}
		return w
	}
	cases := []struct {
		name    string
		prior   int
		current int64
		want    bool
	}{
		{"above five times the median fires", 5, 5001, true},
		{"exactly five times does not", 5, 5000, false},
		{"only four prior does not", 4, 9000, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := append(history(c.prior), tx("z", 10*h, c.current, "m1", "US"))
			if got := slices.Contains(fired(t, w, last(w)), AmountSpike); got != c.want {
				t.Fatalf("amount_spike fired=%v want %v", got, c.want)
			}
		})
	}
}

func TestNewMerchantHighAmount(t *testing.T) {
	cases := []struct {
		name     string
		merchant string
		cents    int64
		want     bool
	}{
		{"new merchant above $500 fires", "m9", 50001, true},
		{"exactly $500 does not", "m9", 50000, false},
		{"known merchant does not", "m1", 90000, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := []domain.Tx{tx("a", 0, 1000, "m1", "US"), tx("b", time.Hour, c.cents, c.merchant, "US")}
			if got := slices.Contains(fired(t, w, last(w)), NewMerchantHighAmount); got != c.want {
				t.Fatalf("new_merchant_high_amount fired=%v want %v", got, c.want)
			}
		})
	}
}

func TestEvaluateIgnoresLaterEntriesAndItself(t *testing.T) {
	s := time.Second
	w := series(0, 1*s, 2*s, 3*s, 4*s, 5*s) // six within 60s
	current := w[1]                          // only one earlier tx exists for it
	if names := fired(t, w, current); slices.Contains(names, Velocity) {
		t.Fatalf("later transactions must not count: %v", names)
	}
}

func TestEvaluateCanFireSeveralRules(t *testing.T) {
	w := []domain.Tx{tx("a", 0, 1000, "m1", "IN"), tx("b", time.Minute, 90000, "m7", "US")}
	names := fired(t, w, last(w))
	for _, want := range []string{ImpossibleTravel, NewMerchantHighAmount} {
		if !slices.Contains(names, want) {
			t.Fatalf("missing %s in %v", want, names)
		}
	}
}

func TestCountriesHaveCentroids(t *testing.T) {
	cs := Countries()
	if len(cs) < 25 || !slices.IsSorted(cs) {
		t.Fatalf("countries: %v", cs)
	}
	if d := distanceKm("US", "US"); d != 0 {
		t.Fatalf("self distance %v", d)
	}
	if d := distanceKm("IN", "US"); math.Abs(d-13000) > 1500 {
		t.Fatalf("IN-US distance %v looks wrong", d)
	}
}
