-- +goose Up
CREATE TABLE processed (
    tx_id        text PRIMARY KEY,
    partition    int NOT NULL,
    worker       int NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE alerts (
    id           bigserial PRIMARY KEY,
    tx_id        text NOT NULL UNIQUE REFERENCES processed (tx_id),
    card_id      text NOT NULL,
    amount_cents bigint NOT NULL,
    score        int NOT NULL,
    severity     text NOT NULL,
    rules        text[] NOT NULL,
    details      jsonb NOT NULL,
    event_time   timestamptz NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX alerts_rules_idx ON alerts USING gin (rules);
CREATE TABLE minute_stats (
    minute       timestamptz PRIMARY KEY,
    scored       int NOT NULL DEFAULT 0,
    alerts       int NOT NULL DEFAULT 0,
    deduplicated int NOT NULL DEFAULT 0
);
CREATE TABLE runs (
    id         bigserial PRIMARY KEY,
    started_at timestamptz NOT NULL,
    ends_at    timestamptz NOT NULL,
    status     text NOT NULL
);
CREATE INDEX runs_started_at_idx ON runs (started_at);

-- +goose Down
DROP TABLE runs;
DROP TABLE minute_stats;
DROP TABLE alerts;
DROP TABLE processed;
