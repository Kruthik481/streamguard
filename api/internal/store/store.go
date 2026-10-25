// Package store persists the processed ledger, alerts, per-minute stats and runs in Postgres.
package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"

	"github.com/Kruthik481/streamguard/api/internal/domain"
)

const pageSize = 50

//go:embed migrations/*.sql
var migrations embed.FS

func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	sub, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return err
	}
	db := stdlib.OpenDBFromPool(pool)
	defer db.Close()
	p, err := goose.NewProvider(database.DialectPostgres, db, sub)
	if err != nil {
		return err
	}
	_, err = p.Up(ctx)
	return err
}

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

type Alert struct {
	ID          int64        `json:"id"`
	TxID        string       `json:"tx_id"`
	CardID      string       `json:"card_id"`
	AmountCents int64        `json:"amount_cents"`
	Score       int          `json:"score"`
	Severity    string       `json:"severity"`
	Rules       []string     `json:"rules"`
	Details     []domain.Hit `json:"details"`
	EventTime   time.Time    `json:"event_time"`
	CreatedAt   time.Time    `json:"created_at"`
}

type MinuteStat struct {
	Minute       time.Time `json:"minute"`
	Scored       int       `json:"scored"`
	Alerts       int       `json:"alerts"`
	Deduplicated int       `json:"deduplicated"`
}

// Record writes tx's outcome exactly once. inserted is false when tx was already processed, in
// which case only the minute's deduplicated counter moves.
func (s *Store) Record(ctx context.Context, tx domain.Tx, hits []domain.Hit, meta domain.Meta) (bool, error) {
	minute := time.Now().UTC().Truncate(time.Minute)
	inserted := false
	err := pgx.BeginFunc(ctx, s.pool, func(q pgx.Tx) error {
		tag, err := q.Exec(ctx,
			`INSERT INTO processed (tx_id, partition, worker) VALUES ($1, $2, $3) ON CONFLICT (tx_id) DO NOTHING`,
			tx.ID, meta.Partition, meta.Worker)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			_, err = q.Exec(ctx, `INSERT INTO minute_stats (minute, deduplicated) VALUES ($1, 1)
				ON CONFLICT (minute) DO UPDATE SET deduplicated = minute_stats.deduplicated + 1`, minute)
			return err
		}
		inserted = true
		alerted := 0
		if len(hits) > 0 {
			alerted = 1
			score := domain.Score(hits)
			names := make([]string, len(hits))
			for i, h := range hits {
				names[i] = h.Rule
			}
			if _, err := q.Exec(ctx, `INSERT INTO alerts (tx_id, card_id, amount_cents, score, severity, rules, details, event_time)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
				tx.ID, tx.CardID, tx.AmountCents, score, domain.Severity(score), names, hits, tx.EventTime); err != nil {
				return err
			}
		}
		_, err = q.Exec(ctx, `INSERT INTO minute_stats (minute, scored, alerts) VALUES ($1, 1, $2)
			ON CONFLICT (minute) DO UPDATE SET scored = minute_stats.scored + 1, alerts = minute_stats.alerts + $2`,
			minute, alerted)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("record %s: %w", tx.ID, err)
	}
	return inserted, nil
}

func (s *Store) ListAlerts(ctx context.Context, cursor int64, rule string) ([]Alert, int64, error) {
	rows, err := s.pool.Query(ctx, `SELECT id, tx_id, card_id, amount_cents, score, severity, rules, details, event_time, created_at
		FROM alerts WHERE ($1::bigint = 0 OR id < $1) AND ($2::text = '' OR $2 = ANY (rules))
		ORDER BY id DESC LIMIT $3`, cursor, rule, pageSize+1)
	if err != nil {
		return nil, 0, err
	}
	alerts, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (Alert, error) {
		var a Alert
		err := r.Scan(&a.ID, &a.TxID, &a.CardID, &a.AmountCents, &a.Score, &a.Severity, &a.Rules, &a.Details, &a.EventTime, &a.CreatedAt)
		return a, err
	})
	if err != nil {
		return nil, 0, err
	}
	if len(alerts) > pageSize {
		return alerts[:pageSize], alerts[pageSize-1].ID, nil
	}
	return alerts, 0, nil
}

func (s *Store) MinuteStats(ctx context.Context, since time.Time) ([]MinuteStat, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT minute, scored, alerts, deduplicated FROM minute_stats WHERE minute >= $1 ORDER BY minute`, since)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[MinuteStat])
}

// PurgeProcessed deletes ledger rows older than before, except those an alert still references.
func (s *Store) PurgeProcessed(ctx context.Context, before time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM processed p WHERE processed_at < $1
		AND NOT EXISTS (SELECT 1 FROM alerts a WHERE a.tx_id = p.tx_id)`, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
