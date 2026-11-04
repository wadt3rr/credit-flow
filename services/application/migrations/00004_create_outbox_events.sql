-- +goose Up
CREATE TABLE outbox_events (
    id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    attempts INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    locked_until TIMESTAMPTZ,
    locked_by UUID,
    
    CONSTRAINT chk_locked_state CHECK (
        (locked_until IS NULL AND locked_by IS NULL)
        OR
        (locked_until IS NOT NULL AND locked_by IS NOT NULL)
    ),

    CONSTRAINT chk_published_not_locked CHECK (
        published_at IS NULL
        OR
        (locked_until IS NULL AND locked_by IS NULL)
    )

);


-- +goose Down
DROP TABLE IF EXISTS outbox_events;