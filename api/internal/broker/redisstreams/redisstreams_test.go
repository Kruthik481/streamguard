package redisstreams_test

import (
	"testing"

	"github.com/Kruthik481/streamguard/api/internal/broker"
	"github.com/Kruthik481/streamguard/api/internal/broker/brokertest"
	"github.com/Kruthik481/streamguard/api/internal/broker/redisstreams"
	"github.com/Kruthik481/streamguard/api/internal/testutil"
)

func TestRedisStreamsContract(t *testing.T) {
	brokertest.Run(t, func(t *testing.T) broker.Broker {
		rdb, prefix := testutil.Redis(t)
		b := redisstreams.New(rdb, prefix, 4)
		if err := b.Setup(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := b.Setup(t.Context()); err != nil { // idempotent
			t.Fatal(err)
		}
		return b
	})
}
