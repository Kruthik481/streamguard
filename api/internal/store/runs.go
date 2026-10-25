package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const runLockKey = 7331 // advisory lock serialising run starts

var ErrDailyCap = errors.New("daily run cap reached")

type ActiveError struct{ EndsAt time.Time }

func (e *ActiveError) Error() string {
	return fmt.Sprintf("a run is active until %s", e.EndsAt.Format(time.RFC3339))
}

type Run struct {
	ID        int64     `json:"id"`
	StartedAt time.Time `json:"started_at"`
	EndsAt    time.Time `json:"ends_at"`
	Status    string    `json:"status"`
}

func (s *Store) StartRun(ctx context.Context, now time.Time, dur time.Duration, dailyCap int) (Run, error) {
	now = now.UTC()
	dayStart := now.Truncate(24 * time.Hour)
	var run Run
	err := pgx.BeginFunc(ctx, s.pool, func(q pgx.Tx) error {
		if _, err := q.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, runLockKey); err != nil {
			return err
		}
		var endsAt time.Time
		err := q.QueryRow(ctx, `SELECT ends_at FROM runs WHERE status = 'running' AND ends_at > $1
			ORDER BY id DESC LIMIT 1`, now).Scan(&endsAt)
		if err == nil {
			return &ActiveError{EndsAt: endsAt.UTC()}
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		var today int
		if err := q.QueryRow(ctx, `SELECT count(*) FROM runs WHERE started_at >= $1 AND started_at < $2`,
			dayStart, dayStart.Add(24*time.Hour)).Scan(&today); err != nil {
			return err
		}
		if today >= dailyCap {
			return ErrDailyCap
		}
		return q.QueryRow(ctx, `INSERT INTO runs (started_at, ends_at, status) VALUES ($1, $2, 'running')
			RETURNING id, started_at, ends_at, status`, now, now.Add(dur)).
			Scan(&run.ID, &run.StartedAt, &run.EndsAt, &run.Status)
	})
	run.StartedAt, run.EndsAt = run.StartedAt.UTC(), run.EndsAt.UTC()
	return run, err
}

func (s *Store) FinishRun(ctx context.Context, id int64, status string) error {
	_, err := s.pool.Exec(ctx, `UPDATE runs SET status = $2 WHERE id = $1`, id, status)
	return err
}

// AbandonRunning closes runs left 'running' by a process that died; called once at startup.
func (s *Store) AbandonRunning(ctx context.Context) (int64, error) {
	tag, err := s.pool.Exec(ctx, `UPDATE runs SET status = 'abandoned' WHERE status = 'running'`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
