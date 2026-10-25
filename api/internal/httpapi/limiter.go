package httpapi

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisLimiter is a fixed one-minute window per key: INCR + EXPIRE in one round trip.
type RedisLimiter struct {
	rdb    redis.UniversalClient
	prefix string
	limit  int64
	now    func() time.Time
}

func NewLimiter(rdb redis.UniversalClient, prefix string, perMinute int) *RedisLimiter {
	return &RedisLimiter{rdb: rdb, prefix: prefix, limit: int64(perMinute), now: time.Now}
}

func (l *RedisLimiter) Allow(ctx context.Context, key string) (bool, time.Duration, error) {
	now := l.now().UTC()
	minute := now.Truncate(time.Minute)
	k := fmt.Sprintf("%srl:%s:%d", l.prefix, key, minute.Unix())
	pipe := l.rdb.Pipeline()
	count := pipe.Incr(ctx, k)
	pipe.Expire(ctx, k, 70*time.Second)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, 0, err
	}
	return count.Val() <= l.limit, minute.Add(time.Minute).Sub(now), nil
}
