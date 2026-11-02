BEGIN;

CREATE TABLE IF NOT EXISTS order_creation_tasks (
    operation_id UUID PRIMARY KEY,

    uid INTEGER NOT NULL CHECK (uid > 0),

    actor_role VARCHAR(50) NOT NULL
        CHECK (actor_role IN (
            'CUSTOMER',
            'ADMIN',
            'SHIPMENT_STAFF'
        )),

    items JSONB NOT NULL
        CHECK (
            CASE
                WHEN jsonb_typeof(items) = 'array'
                THEN jsonb_array_length(items) > 0
                ELSE FALSE
            END
        ),

    score INTEGER CHECK (score >= 0),

    status VARCHAR(30) NOT NULL DEFAULT 'PROCESSING'
        CHECK (status IN (
            'PROCESSING',
            'CONFIRM_PENDING',
            'COMPLETED',
            'RELEASE_PENDING',
            'FAILED'
        )),

    failure_code TEXT,

    attempts INTEGER NOT NULL DEFAULT 0
        CHECK (attempts >= 0),

    last_error_code TEXT,

    next_attempt_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    created_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP,

    updated_at TIMESTAMPTZ NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS operation_id UUID
        REFERENCES order_creation_tasks(operation_id);

CREATE UNIQUE INDEX IF NOT EXISTS orders_operation_id_unique
    ON orders (operation_id)
    WHERE operation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS order_creation_tasks_pending_idx
    ON order_creation_tasks (next_attempt_at, created_at)
    WHERE status IN (
        'PROCESSING',
        'CONFIRM_PENDING',
        'RELEASE_PENDING'
    );

COMMIT;