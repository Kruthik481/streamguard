//go:build redpanda

package redpanda_test

import (
	"os"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"

	"github.com/Kruthik481/streamguard/api/internal/broker"
	"github.com/Kruthik481/streamguard/api/internal/broker/brokertest"
	"github.com/Kruthik481/streamguard/api/internal/broker/redpanda"
)

func TestRedpandaContract(t *testing.T) {
	seeds := os.Getenv("TEST_REDPANDA_BROKERS")
	if seeds == "" {
		seeds = "localhost:19092"
	}
	brokertest.Run(t, func(t *testing.T) broker.Broker {
		topic := "sgtest-" + strings.ToLower(ulid.Make().String())
		b, err := redpanda.New(strings.Split(seeds, ","), topic, 4)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(b.Close)
		if err := b.Setup(t.Context()); err != nil {
			t.Fatal(err)
		}
		return b
	})
}
