// Package httpapi exposes run control, alert history, health checks and the live SSE stream.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/gen"
	"github.com/Kruthik481/streamguard/api/internal/hub"
	"github.com/Kruthik481/streamguard/api/internal/run"
	"github.com/Kruthik481/streamguard/api/internal/scorer"
	"github.com/Kruthik481/streamguard/api/internal/store"
)

const checkTimeout = 2 * time.Second

type Controller interface {
	Start(ctx context.Context) (store.Run, error)
	Inject(ctx context.Context, name string) error
	Kill(id int) error
	View() *run.View
}

type Alerts interface {
	ListAlerts(ctx context.Context, cursor int64, rule string) ([]store.Alert, int64, error)
	MinuteStats(ctx context.Context, since time.Time) ([]store.MinuteStat, error)
}

type Limiter interface {
	Allow(ctx context.Context, key string) (bool, time.Duration, error)
}

type Deps struct {
	Ctl       Controller
	Alerts    Alerts
	Hub       *hub.Hub
	Limiter   Limiter
	Origin    string
	Checks    map[string]func(context.Context) error
	KeepAlive time.Duration
	Log       *slog.Logger
}

type server struct{ d Deps }

func New(d Deps) http.Handler {
	s := &server{d: d}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /events", s.events)
	mux.HandleFunc("GET /alerts", s.alerts)
	mux.Handle("POST /runs", s.limited(s.startRun))
	mux.Handle("POST /runs/current/scenarios/{name}", s.limited(s.inject))
	mux.Handle("POST /runs/current/workers/{id}/kill", s.limited(s.kill))
	return s.cors(mux)
}

func (s *server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Origin") == s.d.Origin {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", s.d.Origin)
			h.Add("Vary", "Origin")
			if r.Method == http.MethodOptions {
				h.Set("Access-Control-Allow-Methods", "GET, POST")
				h.Set("Access-Control-Allow-Headers", "Content-Type")
				h.Set("Access-Control-Max-Age", "600")
			}
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) limited(h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok, retry, err := s.d.Limiter.Allow(r.Context(), clientIP(r))
		if err != nil {
			s.d.Log.Error("rate limiter unavailable", "err", err)
			writeError(w, http.StatusServiceUnavailable, "service_unavailable", "The demo's control store is unreachable. Try again shortly.", nil)
			return
		}
		if !ok {
			w.Header().Set("Retry-After", strconv.Itoa(int(math.Ceil(retry.Seconds()))))
			writeError(w, http.StatusTooManyRequests, "rate_limited", "Too many requests; slow down a little.", nil)
			return
		}
		h(w, r)
	})
}

// clientIP prefers proxy headers. shortcut: they are spoofable; fine behind Render, where the
// daily run cap is the real budget guard.
func clientIP(r *http.Request) string {
	if ip := r.Header.Get("CF-Connecting-IP"); ip != "" {
		return ip
	}
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.Split(xff, ",")[0])
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	result := map[string]bool{}
	ok := true
	for name, check := range s.d.Checks {
		ctx, cancel := context.WithTimeout(r.Context(), checkTimeout)
		err := check(ctx)
		cancel()
		result[name] = err == nil
		ok = ok && err == nil
	}
	result["ok"] = ok
	status := http.StatusOK
	if !ok {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, result)
}

func (s *server) startRun(w http.ResponseWriter, r *http.Request) {
	started, err := s.d.Ctl.Start(r.Context())
	var active *store.ActiveError
	switch {
	case err == nil:
		writeJSON(w, http.StatusCreated, started)
	case errors.As(err, &active):
		writeError(w, http.StatusConflict, "run_active", "A run is already in progress; watch it live.",
			map[string]any{"ends_at": active.EndsAt})
	case errors.Is(err, store.ErrDailyCap):
		writeError(w, http.StatusTooManyRequests, "daily_cap", "Today's demo runs are used up. Try again after 00:00 UTC.", nil)
	default:
		s.internal(w, err)
	}
}

func (s *server) inject(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	err := s.d.Ctl.Inject(r.Context(), name)
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, map[string]string{"scenario": name})
	case errors.Is(err, run.ErrNoRun):
		writeError(w, http.StatusConflict, "no_run", "Start a run first.", nil)
	case errors.Is(err, gen.ErrUnknownScenario):
		writeError(w, http.StatusNotFound, "unknown_scenario", "Unknown scenario.", nil)
	default:
		s.internal(w, err)
	}
}

func (s *server) kill(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, "unknown_worker", "Unknown worker.", nil)
		return
	}
	err = s.d.Ctl.Kill(id)
	switch {
	case err == nil:
		writeJSON(w, http.StatusAccepted, map[string]int{"killed": id})
	case errors.Is(err, run.ErrNoRun):
		writeError(w, http.StatusConflict, "no_run", "Start a run first.", nil)
	case errors.Is(err, scorer.ErrUnknownWorker):
		writeError(w, http.StatusNotFound, "unknown_worker", "Unknown worker.", nil)
	case errors.Is(err, scorer.ErrWorkerDown):
		writeError(w, http.StatusConflict, "already_down", "That worker is already down; it restarts shortly.", nil)
	case errors.Is(err, scorer.ErrLastWorker):
		writeError(w, http.StatusConflict, "last_worker", "At least one worker must stay up.", nil)
	default:
		s.internal(w, err)
	}
}

func (s *server) alerts(w http.ResponseWriter, r *http.Request) {
	var cursor int64
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		c, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || c < 0 {
			writeError(w, http.StatusBadRequest, "bad_cursor", "cursor must be a non-negative integer.", nil)
			return
		}
		cursor = c
	}
	list, next, err := s.d.Alerts.ListAlerts(r.Context(), cursor, r.URL.Query().Get("rule"))
	if err != nil {
		s.internal(w, err)
		return
	}
	if list == nil {
		list = []store.Alert{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"alerts": list, "next_cursor": next})
}

func (s *server) internal(w http.ResponseWriter, err error) {
	s.d.Log.Error("request failed", "err", err)
	writeError(w, http.StatusInternalServerError, "internal_error", "Something went wrong on our side.", nil)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string, extra map[string]any) {
	body := map[string]any{"code": code, "message": message}
	for k, v := range extra {
		body[k] = v
	}
	writeJSON(w, status, map[string]any{"error": body})
}
