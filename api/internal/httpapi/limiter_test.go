package httpapi

import (
	"testing"
	"time"
)

func TestLimiterAllowsTenPerMinute(t *testing.T) {
	l := NewLimiter(10)
	l.now = func() time.Time { return time.Date(2026, 10, 10, 12, 0, 30, 0, time.UTC) }
	for i := range 10 {
		if ok, _, err := l.Allow(t.Context(), "1.2.3.4"); !ok || err != nil {
			t.Fatalf("request %d refused: %v", i, err)
		}
	}
	ok, retry, err := l.Allow(t.Context(), "1.2.3.4")
	if ok || err != nil || retry != 30*time.Second {
		t.Fatalf("11th: ok=%v retry=%v err=%v", ok, retry, err)
	}
	if ok, _, _ := l.Allow(t.Context(), "5.6.7.8"); !ok {
		t.Fatal("other clients have their own budget")
	}
}

func TestLimiterResetsEachMinuteAndForgetsOldWindows(t *testing.T) {
	l := NewLimiter(1)
	now := time.Date(2026, 10, 10, 12, 0, 59, 0, time.UTC)
	l.now = func() time.Time { return now }
	if ok, _, _ := l.Allow(t.Context(), "a"); !ok {
		t.Fatal("first request refused")
	}
	if ok, _, _ := l.Allow(t.Context(), "a"); ok {
		t.Fatal("second request in the same minute allowed")
	}
	now = now.Add(2 * time.Second)
	if ok, _, _ := l.Allow(t.Context(), "a"); !ok {
		t.Fatal("new minute must reset the budget")
	}
	if len(l.counts) != 1 {
		t.Fatalf("old windows must be dropped, have %d keys", len(l.counts))
	}
}
