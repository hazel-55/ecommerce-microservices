BEGIN;

CREATE TABLE IF NOT EXISTS orders (
    oid INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uid INTEGER NOT NULL CHECK (uid > 0),
    discount NUMERIC(18, 2) NOT NULL DEFAULT 0
        CHECK (discount >= 0)
);

CREATE TABLE IF NOT EXISTS order_details (
    oid INTEGER NOT NULL REFERENCES orders(oid),
    pid INTEGER NOT NULL CHECK (pid > 0),
    qty INTEGER NOT NULL CHECK (qty > 0),
    unit_price NUMERIC(18, 2) NOT NULL
        CHECK (unit_price >= 0),
    PRIMARY KEY (oid, pid)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    oid INTEGER NOT NULL REFERENCES orders(oid),
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS outbox_events_pending_idx
    ON outbox_events (next_attempt_at, created_at)
    WHERE published_at IS NULL;

COMMIT;