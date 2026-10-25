// Command streamguard runs the generator, the scorer workers and the HTTP API in one process.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/Kruthik481/streamguard/api/internal/broker"
	"github.com/Kruthik481/streamguard/api/internal/broker/redisstreams"
	"github.com/Kruthik481/streamguard/api/internal/broker/redpanda"
	"github.com/Kruthik481/streamguard/api/internal/config"
	"github.com/Kruthik481/streamguard/api/internal/gen"
	"github.com/Kruthik481/streamguard/api/internal/httpapi"
	"github.com/Kruthik481/streamguard/api/internal/hub"
	"github.com/Kruthik481/streamguard/api/internal/run"
	"github.com/Kruthik481/streamguard/api/internal/scorer"
	"github.com/Kruthik481/streamguard/api/internal/state"
	"github.com/Kruthik481/streamguard/api/internal/store"
)

const (
	controlPerMinute = 10
	keepAlive        = 15 * time.Second
	shutdownTimeout  = 10 * time.Second
	redpandaTopic    = "transactions"
)

var (
	_ run.Engine         = (*scorer.Coordinator)(nil)
	_ run.Source         = (*gen.Generator)(nil)
	_ run.RunStore       = (*store.Store)(nil)
	_ httpapi.Alerts     = (*store.Store)(nil)
	_ httpapi.Controller = (*run.Controller)(nil)
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := serve(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func serve(log *slog.Logger) error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := store.Migrate(ctx, pool); err != nil {
		return err
	}
	st := store.New(pool)
	if n, err := st.AbandonRunning(ctx); err != nil {
		return err
	} else if n > 0 {
		log.Warn("abandoned runs left by a previous process", "runs", n)
	}

	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()

	scfg := scorer.DefaultConfig()
	b, closeBroker, err := newBroker(cfg, rdb, scfg.Partitions)
	if err != nil {
		return err
	}
	defer closeBroker()
	if err := b.Setup(ctx); err != nil {
		return err
	}

	events := hub.New(hub.DefaultBuffer)
	windows := state.New(rdb, cfg.KeyPrefix)
	rcfg := run.DefaultConfig()
	rcfg.Duration, rcfg.PerSecond, rcfg.DailyCap = cfg.RunDuration, cfg.PerSecond, cfg.DailyCap
	ctl := run.New(ctx, rcfg, st,
		func() run.Engine {
			return scorer.New(scfg, scorer.Deps{Broker: b, State: windows, Store: st, Hub: events, Log: log})
		},
		func() run.Source { return gen.New(b, scfg.Partitions, time.Now().UnixNano(), time.Now) },
		events, log)

	handler := httpapi.New(httpapi.Deps{
		Ctl: ctl, Alerts: st, Hub: events, Limiter: httpapi.NewLimiter(controlPerMinute),
		Origin: cfg.WebOrigin, KeepAlive: keepAlive, Log: log,
		Checks: map[string]func(context.Context) error{
			"postgres": st.Ping,
			"redis":    func(ctx context.Context) error { return rdb.Ping(ctx).Err() },
		},
	})
	// No WriteTimeout: /events is a long-lived stream.
	srv := &http.Server{Addr: ":" + cfg.Port, Handler: handler, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 2 * time.Minute}
	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "port", cfg.Port, "broker", cfg.Broker)
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	ctl.Shutdown(sctx)
	events.Close() // ends SSE handlers so Shutdown does not wait on them
	if err := srv.Shutdown(sctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func newBroker(cfg config.Config, rdb *redis.Client, partitions int) (broker.Broker, func(), error) {
	if cfg.Broker == "redpanda" {
		b, err := redpanda.New(cfg.RedpandaBrokers, redpandaTopic, partitions)
		if err != nil {
			return nil, nil, err
		}
		return b, b.Close, nil
	}
	return redisstreams.New(rdb, cfg.KeyPrefix, partitions), func() {}, nil
}
