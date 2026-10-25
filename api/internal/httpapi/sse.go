package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Kruthik481/streamguard/api/internal/hub"
	"github.com/Kruthik481/streamguard/api/internal/run"
	"github.com/Kruthik481/streamguard/api/internal/store"
)

const statsLookback = 30 * time.Minute

type snapshot struct {
	Run    *run.View          `json:"run"`
	Alerts []store.Alert      `json:"alerts"`
	Stats  []store.MinuteStat `json:"stats"`
}

func (s *server) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		s.internal(w, fmt.Errorf("response writer cannot flush"))
		return
	}
	ch, cancel := s.d.Hub.Subscribe() // subscribe before the snapshot so no event falls in between
	defer cancel()
	snap, err := s.snapshot(r.Context())
	if err != nil {
		s.internal(w, err)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	if writeEvent(w, hub.Event{Type: "snapshot", Data: snap}) != nil {
		return
	}
	flusher.Flush()
	keepAlive := time.NewTicker(s.d.KeepAlive)
	defer keepAlive.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case e, open := <-ch:
			if !open { // dropped as too slow, or shutting down; EventSource reconnects
				return
			}
			if writeEvent(w, e) != nil {
				return
			}
			flusher.Flush()
		case <-keepAlive.C:
			if _, err := io.WriteString(w, ": keep-alive\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

func (s *server) snapshot(ctx context.Context) (snapshot, error) {
	alerts, _, err := s.d.Alerts.ListAlerts(ctx, 0, "")
	if err != nil {
		return snapshot{}, err
	}
	stats, err := s.d.Alerts.MinuteStats(ctx, time.Now().Add(-statsLookback))
	if err != nil {
		return snapshot{}, err
	}
	if alerts == nil {
		alerts = []store.Alert{}
	}
	if stats == nil {
		stats = []store.MinuteStat{}
	}
	return snapshot{Run: s.d.Ctl.View(), Alerts: alerts, Stats: stats}, nil
}

func writeEvent(w io.Writer, e hub.Event) error {
	data, err := json.Marshal(e.Data)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Type, data)
	return err
}
