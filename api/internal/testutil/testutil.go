// Package testutil gives integration tests an isolated Redis key prefix and Postgres schema.
package testutil

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/oklog/ulid/v2"
	"github.com/redis/go-redis/v9"
)

const (
	defaultRedisURL    = "redis://localhost:6381/0"
	defaultDatabaseURL = "postgres://streamguard:streamguard@localhost:5435/streamguard_test?sslmode=disable"
)

// Redis returns a client and a key prefix unique to this test; keys under it are deleted afterwards.
func Redis(t *testing.T) (*redis.Client, string) {
	t.Helper()
	opts, err := redis.ParseURL(getenv("TEST_REDIS_URL", defaultRedisURL))
	if err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(t.Context()).Err(); err != nil {
		t.Fatalf("redis unavailable (run `docker compose up -d --wait`): %v", err)
	}
	prefix := "sgtest:" + strings.ToLower(ulid.Make().String()) + ":"
	t.Cleanup(func() {
		ctx := context.Background()
		iter := rdb.Scan(ctx, 0, prefix+"*", 500).Iterator()
		for iter.Next(ctx) {
			rdb.Del(ctx, iter.Val())
		}
		_ = rdb.Close()
	})
	return rdb, prefix
}

// Postgres returns a pool whose search_path is a fresh schema with all migrations applied.
// migrate is store.Migrate; it is passed in to avoid an import cycle with package store.
func Postgres(t *testing.T, migrate func(context.Context, *pgxpool.Pool) error) *pgxpool.Pool {
	t.Helper()
	ctx := t.Context()
	url := getenv("TEST_DATABASE_URL", defaultDatabaseURL)
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("postgres unavailable (run `docker compose up -d --wait`): %v", err)
	}
	schema := "t_" + strings.ToLower(ulid.Make().String())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(context.Background())
	})
	if err := migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool
}

func getenv(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}
