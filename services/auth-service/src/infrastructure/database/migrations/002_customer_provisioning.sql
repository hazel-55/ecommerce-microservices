BEGIN;

CREATE TABLE IF NOT EXISTS customer_provisioning_tasks (
    uid INTEGER PRIMARY KEY REFERENCES users(uid),
    fullname VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS customer_provisioning_pending_idx
    ON customer_provisioning_tasks (next_attempt_at, created_at)
    WHERE completed_at IS NULL;

COMMIT;