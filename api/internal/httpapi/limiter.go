package httpapi

import (
	"context"
	"sync"
	"time"
)

// MemoryLimiter is a fixed one-minute window per key, kept in process. The service runs as a
// single instance, and counting in memory means rejected requests cost no Redis commands, so a
// flood of POSTs cannot drain the shared Upstash quota.
type MemoryLimiter struct {
	mu     sync.Mutex
	limit  int
	window time.Time
	counts map[string]int
	now    func() time.Time
}

func NewLimiter(perMinute int) *MemoryLimiter {
	return &MemoryLimiter{limit: perMinute, counts: map[string]int{}, now: time.Now}
}

func (l *MemoryLimiter) Allow(_ context.Context, key string) (bool, time.Duration, error) {
	now := l.now().UTC()
	minute := now.Truncate(time.Minute)
	l.mu.Lock()
	defer l.mu.Unlock()
	if !minute.Equal(l.window) {
		l.window, l.counts = minute, map[string]int{}
	}
	l.counts[key]++
	return l.counts[key] <= l.limit, minute.Add(time.Minute).Sub(now), nil
}
