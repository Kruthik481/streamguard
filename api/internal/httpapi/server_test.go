package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/gen"
	"github.com/Kruthik481/streamguard/api/internal/hub"
	"github.com/Kruthik481/streamguard/api/internal/run"
	"github.com/Kruthik481/streamguard/api/internal/scorer"
	"github.com/Kruthik481/streamguard/api/internal/store"
)

const origin = "http://localhost:3200"

type fakeCtl struct {
	startErr, injectErr, killErr error
	view                         *run.View
}

func (f *fakeCtl) Start(context.Context) (store.Run, error) {
	if f.startErr != nil {
		return store.Run{}, f.startErr
	}
	return store.Run{ID: 1, Status: "running"}, nil
}
func (f *fakeCtl) Inject(context.Context, string) error { return f.injectErr }
func (f *fakeCtl) Kill(int) error                       { return f.killErr }
func (f *fakeCtl) View() *run.View                      { return f.view }

type fakeAlerts struct {
	gotCursor int64
	gotRule   string
}

func (f *fakeAlerts) ListAlerts(_ context.Context, cursor int64, rule string) ([]store.Alert, int64, error) {
	f.gotCursor, f.gotRule = cursor, rule
	return []store.Alert{{ID: 9, TxID: "t9", Rules: []string{"velocity"}}}, 0, nil
}
func (f *fakeAlerts) MinuteStats(context.Context, time.Time) ([]store.MinuteStat, error) {
	return []store.MinuteStat{{Scored: 4}}, nil
}

type fakeLimiter struct {
	allow bool
	err   error
}

func (f fakeLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	return f.allow, 30 * time.Second, f.err
}

type env struct {
	ctl    *fakeCtl
	alerts *fakeAlerts
	hub    *hub.Hub
	h      http.Handler
}

func newEnv(lim Limiter, checks map[string]func(context.Context) error) *env {
	e := &env{ctl: &fakeCtl{}, alerts: &fakeAlerts{}, hub: hub.New(16)}
	e.h = New(Deps{Ctl: e.ctl, Alerts: e.alerts, Hub: e.hub, Limiter: lim, Origin: origin, Checks: checks,
		KeepAlive: 20 * time.Millisecond, Log: slog.New(slog.DiscardHandler)})
	return e
}

func do(h http.Handler, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Origin", origin)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func errorCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("not an error body: %s", rec.Body)
	}
	return body.Error.Code
}

func TestStartRunResponses(t *testing.T) {
	e := newEnv(fakeLimiter{allow: true}, nil)
	if rec := do(e.h, "POST", "/runs"); rec.Code != 201 {
		t.Fatalf("start: %d %s", rec.Code, rec.Body)
	}
	ends := time.Date(2026, 10, 10, 12, 3, 0, 0, time.UTC)
	e.ctl.startErr = &store.ActiveError{EndsAt: ends}
	rec := do(e.h, "POST", "/runs")
	if rec.Code != 409 || errorCode(t, rec) != "run_active" || !strings.Contains(rec.Body.String(), "2026-10-10T12:03:00Z") {
		t.Fatalf("active: %d %s", rec.Code, rec.Body)
	}
	e.ctl.startErr = store.ErrDailyCap
	if rec := do(e.h, "POST", "/runs"); rec.Code != 429 || errorCode(t, rec) != "daily_cap" {
		t.Fatalf("cap: %d %s", rec.Code, rec.Body)
	}
	e.ctl.startErr = errors.New("db down")
	if rec := do(e.h, "POST", "/runs"); rec.Code != 500 || errorCode(t, rec) != "internal_error" {
		t.Fatalf("internal: %d %s", rec.Code, rec.Body)
	}
}

func TestScenarioAndKillResponses(t *testing.T) {
	e := newEnv(fakeLimiter{allow: true}, nil)
	inject := func(err error) { e.ctl.injectErr = err }
	kill := func(err error) { e.ctl.killErr = err }
	cases := []struct {
		path string
		err  error
		set  func(error)
		code int
		want string
	}{
		{"/runs/current/scenarios/card_testing", nil, inject, 202, ""},
		{"/runs/current/scenarios/card_testing", run.ErrNoRun, inject, 409, "no_run"},
		{"/runs/current/scenarios/nope", gen.ErrUnknownScenario, inject, 404, "unknown_scenario"},
		{"/runs/current/workers/2/kill", nil, kill, 202, ""},
		{"/runs/current/workers/2/kill", scorer.ErrWorkerDown, kill, 409, "already_down"},
		{"/runs/current/workers/2/kill", scorer.ErrLastWorker, kill, 409, "last_worker"},
		{"/runs/current/workers/9/kill", scorer.ErrUnknownWorker, kill, 404, "unknown_worker"},
		{"/runs/current/workers/x/kill", nil, kill, 404, "unknown_worker"},
		{"/runs/current/workers/1/kill", run.ErrNoRun, kill, 409, "no_run"},
	}
	for _, c := range cases {
		c.set(c.err)
		rec := do(e.h, "POST", c.path)
		if rec.Code != c.code || (c.want != "" && errorCode(t, rec) != c.want) {
			t.Fatalf("%s with %v: %d %s", c.path, c.err, rec.Code, rec.Body)
		}
	}
}

func TestRateLimitAndLimiterOutage(t *testing.T) {
	limited := newEnv(fakeLimiter{allow: false}, nil)
	rec := do(limited.h, "POST", "/runs")
	if rec.Code != 429 || errorCode(t, rec) != "rate_limited" || rec.Header().Get("Retry-After") != "30" {
		t.Fatalf("limited: %d %v %s", rec.Code, rec.Header(), rec.Body)
	}
	down := newEnv(fakeLimiter{err: errors.New("redis down")}, nil)
	if rec := do(down.h, "POST", "/runs"); rec.Code != 503 || errorCode(t, rec) != "service_unavailable" {
		t.Fatalf("limiter down: %d %s", rec.Code, rec.Body)
	}
}

func TestCORS(t *testing.T) {
	e := newEnv(fakeLimiter{allow: true}, nil)
	rec := do(e.h, "OPTIONS", "/runs")
	if rec.Code != 204 || rec.Header().Get("Access-Control-Allow-Origin") != origin {
		t.Fatalf("preflight: %d %v", rec.Code, rec.Header())
	}
	req := httptest.NewRequest("GET", "/healthz", nil)
	req.Header.Set("Origin", "https://evil.example")
	other := httptest.NewRecorder()
	e.h.ServeHTTP(other, req)
	if other.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("foreign origins get no CORS header")
	}
}

func TestHealthAndReadiness(t *testing.T) {
	ok := newEnv(fakeLimiter{allow: true}, map[string]func(context.Context) error{
		"redis": func(context.Context) error { return nil },
	})
	if rec := do(ok.h, "GET", "/readyz"); rec.Code != 200 {
		t.Fatalf("ready: %d", rec.Code)
	}
	bad := newEnv(fakeLimiter{allow: true}, map[string]func(context.Context) error{
		"postgres": func(context.Context) error { return errors.New("down") },
	})
	rec := do(bad.h, "GET", "/readyz")
	if rec.Code != 503 || !strings.Contains(rec.Body.String(), `"postgres":false`) {
		t.Fatalf("not ready: %d %s", rec.Code, rec.Body)
	}
	if rec := do(bad.h, "GET", "/healthz"); rec.Code != 200 {
		t.Fatalf("healthz must not depend on dependencies: %d", rec.Code)
	}
}

func TestAlertsPassesCursorAndRule(t *testing.T) {
	e := newEnv(fakeLimiter{allow: true}, nil)
	rec := do(e.h, "GET", "/alerts?cursor=42&rule=velocity")
	if rec.Code != 200 || e.alerts.gotCursor != 42 || e.alerts.gotRule != "velocity" || !strings.Contains(rec.Body.String(), `"next_cursor":0`) {
		t.Fatalf("alerts: %d %s", rec.Code, rec.Body)
	}
	if rec := do(e.h, "GET", "/alerts?cursor=abc"); rec.Code != 400 || errorCode(t, rec) != "bad_cursor" {
		t.Fatalf("bad cursor: %d", rec.Code)
	}
}

func readEvent(t *testing.T, r *bufio.Reader) (string, string) {
	t.Helper()
	var typ, data string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("stream ended: %v", err)
		}
		line = strings.TrimRight(line, "\n")
		switch {
		case strings.HasPrefix(line, "event: "):
			typ = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = strings.TrimPrefix(line, "data: ")
		case strings.HasPrefix(line, ":"):
			typ = "comment"
		case line == "" && typ != "":
			return typ, data
		}
	}
}

func TestEventsSendsSnapshotFirst(t *testing.T) {
	e := newEnv(fakeLimiter{allow: true}, nil)
	e.ctl.view = &run.View{ID: 7, Status: "running", Workers: []scorer.WorkerView{{ID: 0, Up: true, Partitions: []int{0}}}}
	srv := httptest.NewServer(e.h)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	r := bufio.NewReader(resp.Body)
	typ, data := readEvent(t, r)
	if typ != "snapshot" || !strings.Contains(data, `"id":7`) || !strings.Contains(data, `"tx_id":"t9"`) || !strings.Contains(data, `"scored":4`) {
		t.Fatalf("first event %s %s", typ, data)
	}
	e.hub.Publish(hub.Event{Type: "scored", Data: map[string]int{"x": 1}})
	for {
		typ, data = readEvent(t, r)
		if typ != "comment" {
			break
		}
	}
	if typ != "scored" || data != `{"x":1}` {
		t.Fatalf("second event %s %s", typ, data)
	}
}

func TestEventsKeepAliveAndEndWhenHubCloses(t *testing.T) {
	e := newEnv(fakeLimiter{allow: true}, nil)
	srv := httptest.NewServer(e.h)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	r := bufio.NewReader(resp.Body)
	readEvent(t, r) // snapshot (run is null)
	if typ, _ := readEvent(t, r); typ != "comment" {
		t.Fatalf("expected a keep-alive, got %s", typ)
	}
	e.hub.Close()
	if _, err := io.ReadAll(r); err != nil {
		t.Fatalf("stream should end cleanly: %v", err)
	}
}
