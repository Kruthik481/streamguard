package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func base() map[string]string {
	return map[string]string{
		"DATABASE_URL": "postgres://x",
		"REDIS_URL":    "redis://x",
		"WEB_ORIGIN":   "http://localhost:3200",
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	c, err := Load(env(base()))
	if err != nil {
		t.Fatal(err)
	}
	if c.Port != "8200" || c.Broker != "redis" || c.KeyPrefix != "sg:" {
		t.Fatalf("defaults: %+v", c)
	}
	if c.RunDuration != 180*time.Second || c.PerSecond != 5 || c.DailyCap != 4 {
		t.Fatalf("run defaults: %+v", c)
	}
}

func TestLoadRequiresCoreSettings(t *testing.T) {
	_, err := Load(env(map[string]string{}))
	for _, name := range []string{"DATABASE_URL", "REDIS_URL", "WEB_ORIGIN"} {
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("expected %s in error, got %v", name, err)
		}
	}
}

func TestLoadValidatesBrokerAndNumbers(t *testing.T) {
	cases := map[string]map[string]string{
		"unknown broker":           {"BROKER": "nats"},
		"redpanda without brokers": {"BROKER": "redpanda"},
		"zero rate":                {"TX_PER_SECOND": "0"},
		"bad run seconds":          {"RUN_SECONDS": "soon"},
		"negative cap":             {"DAILY_RUN_CAP": "-1"},
	}
	for name, extra := range cases {
		t.Run(name, func(t *testing.T) {
			m := base()
			for k, v := range extra {
				m[k] = v
			}
			if _, err := Load(env(m)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestLoadParsesRedpandaBrokers(t *testing.T) {
	m := base()
	m["BROKER"] = "redpanda"
	m["REDPANDA_BROKERS"] = "a:9092, b:9092"
	c, err := Load(env(m))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.RedpandaBrokers) != 2 || c.RedpandaBrokers[1] != "b:9092" {
		t.Fatalf("brokers: %v", c.RedpandaBrokers)
	}
}
