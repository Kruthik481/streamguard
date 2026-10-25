// Package config reads and validates the service's environment.
package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Port            string
	DatabaseURL     string
	RedisURL        string
	Broker          string
	RedpandaBrokers []string
	WebOrigin       string
	KeyPrefix       string
	RunDuration     time.Duration
	PerSecond       int
	DailyCap        int
}

func Load(getenv func(string) string) (Config, error) {
	c := Config{
		Port:        or(getenv("PORT"), "8200"),
		DatabaseURL: getenv("DATABASE_URL"),
		RedisURL:    getenv("REDIS_URL"),
		Broker:      or(getenv("BROKER"), "redis"),
		WebOrigin:   getenv("WEB_ORIGIN"),
		KeyPrefix:   or(getenv("KEY_PREFIX"), "sg:"),
	}
	var errs []error
	for _, req := range []struct{ name, value string }{
		{"DATABASE_URL", c.DatabaseURL}, {"REDIS_URL", c.RedisURL}, {"WEB_ORIGIN", c.WebOrigin},
	} {
		if req.value == "" {
			errs = append(errs, fmt.Errorf("%s is required", req.name))
		}
	}
	switch c.Broker {
	case "redis":
	case "redpanda":
		for _, b := range strings.Split(getenv("REDPANDA_BROKERS"), ",") {
			if b = strings.TrimSpace(b); b != "" {
				c.RedpandaBrokers = append(c.RedpandaBrokers, b)
			}
		}
		if len(c.RedpandaBrokers) == 0 {
			errs = append(errs, errors.New("REDPANDA_BROKERS is required when BROKER=redpanda"))
		}
	default:
		errs = append(errs, fmt.Errorf("BROKER must be redis or redpanda, got %q", c.Broker))
	}
	seconds, err := positiveInt(getenv, "RUN_SECONDS", 180)
	errs = append(errs, err)
	c.RunDuration = time.Duration(seconds) * time.Second
	c.PerSecond, err = positiveInt(getenv, "TX_PER_SECOND", 5)
	errs = append(errs, err)
	c.DailyCap, err = positiveInt(getenv, "DAILY_RUN_CAP", 4)
	errs = append(errs, err)
	return c, errors.Join(errs...)
}

func positiveInt(getenv func(string) string, name string, def int) (int, error) {
	raw := getenv(name)
	if raw == "" {
		return def, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def, fmt.Errorf("%s must be a positive integer, got %q", name, raw)
	}
	return n, nil
}

func or(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
