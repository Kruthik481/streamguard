package state_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/domain"
	"github.com/Kruthik481/streamguard/api/internal/state"
	"github.com/Kruthik481/streamguard/api/internal/testutil"
)

var t0 = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

func tx(id string, at time.Duration) domain.Tx {
	return domain.Tx{ID: id, CardID: "c1", AmountCents: 1000, MerchantID: "m1", Country: "US", EventTime: t0.Add(at)}
}

func TestApplyIsIdempotent(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	w := state.New(rdb, prefix)
	for range 3 {
		got, err := w.Apply(t.Context(), tx("a", 0))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].ID != "a" {
			t.Fatalf("window = %+v", got)
		}
	}
}

func TestApplyReturnsWindowInEventOrder(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	w := state.New(rdb, prefix)
	for _, x := range []domain.Tx{tx("b", 2*time.Second), tx("a", time.Second), tx("c", 3*time.Second)} {
		if _, err := w.Apply(t.Context(), x); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := w.Apply(t.Context(), tx("c", 3*time.Second))
	if len(got) != 3 || got[0].ID != "a" || got[2].ID != "c" || !got[0].EventTime.Equal(t0.Add(time.Second)) {
		t.Fatalf("window = %+v", got)
	}
}

func TestApplyTrimsByCountAndAge(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	w := state.New(rdb, prefix)
	if _, err := w.Apply(t.Context(), tx("old", -25*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var got []domain.Tx
	for i := range state.MaxEntries + 5 {
		var err error
		got, err = w.Apply(t.Context(), tx(fmt.Sprintf("n%02d", i), time.Duration(i)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != state.MaxEntries || got[0].ID != "n05" {
		t.Fatalf("len=%d first=%s", len(got), got[0].ID)
	}
	ttl := rdb.PTTL(t.Context(), prefix+"card:c1").Val()
	if ttl <= 0 || ttl > state.Span {
		t.Fatalf("ttl = %v", ttl)
	}
}
