package httpapi

import (
	"testing"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/testutil"
)

func TestRedisLimiterAllowsTenPerMinute(t *testing.T) {
	rdb, prefix := testutil.Redis(t)
	l := NewLimiter(rdb, prefix, 10)
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
