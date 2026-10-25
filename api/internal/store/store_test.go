package store_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/domain"
	"github.com/Kruthik481/streamguard/api/internal/store"
	"github.com/Kruthik481/streamguard/api/internal/testutil"
)

var t0 = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

func newStore(t *testing.T) *store.Store {
	return store.New(testutil.Postgres(t, store.Migrate))
}

func tx(id string) domain.Tx {
	return domain.Tx{ID: id, CardID: "c1", AmountCents: 120, MerchantID: "m1", Country: "US", EventTime: t0}
}

var testingHit = []domain.Hit{{Rule: "card_testing", Weight: 3, Detail: "3 charges"}}

func TestRecordWritesOnceAndCountsDuplicates(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	first, err := s.Record(ctx, tx("a"), testingHit, domain.Meta{Partition: 1, Worker: 1})
	if err != nil || !first {
		t.Fatalf("first record = %v, %v", first, err)
	}
	again, err := s.Record(ctx, tx("a"), testingHit, domain.Meta{Partition: 1, Worker: 2})
	if err != nil || again {
		t.Fatalf("duplicate record = %v, %v", again, err)
	}
	alerts, next, err := s.ListAlerts(ctx, 0, "")
	if err != nil || len(alerts) != 1 || next != 0 {
		t.Fatalf("alerts = %d next=%d err=%v", len(alerts), next, err)
	}
	a := alerts[0]
	if a.TxID != "a" || a.Score != 3 || a.Severity != "medium" || a.Rules[0] != "card_testing" || a.Details[0].Detail != "3 charges" {
		t.Fatalf("alert = %+v", a)
	}
	stats, err := s.MinuteStats(ctx, time.Now().Add(-time.Hour))
	var total store.MinuteStat // summed: the two records may straddle a minute boundary
	for _, m := range stats {
		total.Scored, total.Alerts, total.Deduplicated = total.Scored+m.Scored, total.Alerts+m.Alerts, total.Deduplicated+m.Deduplicated
	}
	if err != nil || total.Scored != 1 || total.Alerts != 1 || total.Deduplicated != 1 {
		t.Fatalf("stats = %+v, %v", stats, err)
	}
}

func TestRecordWithoutHitsWritesNoAlert(t *testing.T) {
	s := newStore(t)
	if ok, err := s.Record(t.Context(), tx("a"), nil, domain.Meta{}); err != nil || !ok {
		t.Fatalf("record = %v, %v", ok, err)
	}
	alerts, _, _ := s.ListAlerts(t.Context(), 0, "")
	if len(alerts) != 0 {
		t.Fatalf("alerts = %+v", alerts)
	}
}

func TestConcurrentRecordsOfOneTransactionInsertOnce(t *testing.T) {
	s := newStore(t)
	var wg sync.WaitGroup
	results := make(chan bool, 3)
	for w := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := s.Record(t.Context(), tx("same"), testingHit, domain.Meta{Worker: w})
			if err != nil {
				t.Error(err)
			}
			results <- ok
		}()
	}
	wg.Wait()
	close(results)
	inserted := 0
	for ok := range results {
		if ok {
			inserted++
		}
	}
	if inserted != 1 {
		t.Fatalf("inserted %d times", inserted)
	}
}

func TestListAlertsPagesAndFilters(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	for i := range 55 {
		hits := testingHit
		if i%5 == 0 {
			hits = []domain.Hit{{Rule: "velocity", Weight: 2, Detail: "5 in 60s"}}
		}
		if _, err := s.Record(ctx, tx(fmt.Sprintf("t%02d", i)), hits, domain.Meta{}); err != nil {
			t.Fatal(err)
		}
	}
	page, next, err := s.ListAlerts(ctx, 0, "")
	if err != nil || len(page) != 50 || next == 0 || page[0].TxID != "t54" {
		t.Fatalf("page1 len=%d next=%d err=%v", len(page), next, err)
	}
	rest, next2, _ := s.ListAlerts(ctx, next, "")
	if len(rest) != 5 || next2 != 0 {
		t.Fatalf("page2 len=%d next=%d", len(rest), next2)
	}
	vel, _, _ := s.ListAlerts(ctx, 0, "velocity")
	if len(vel) != 11 {
		t.Fatalf("velocity alerts = %d", len(vel))
	}
}

func TestPurgeKeepsProcessedRowsThatHaveAlerts(t *testing.T) {
	s := newStore(t)
	ctx := t.Context()
	_, _ = s.Record(ctx, tx("quiet"), nil, domain.Meta{})
	_, _ = s.Record(ctx, tx("flagged"), testingHit, domain.Meta{})
	n, err := s.PurgeProcessed(ctx, time.Now().Add(time.Minute))
	if err != nil || n != 1 {
		t.Fatalf("purged %d, %v", n, err)
	}
	if again, _ := s.Record(ctx, tx("flagged"), testingHit, domain.Meta{}); again {
		t.Fatal("flagged tx must still be deduplicated")
	}
}
