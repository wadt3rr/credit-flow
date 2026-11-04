
-- +goose Up
ALTER TABLE outbox_events
    ADD COLUMN status VARCHAR(16),
    ADD COLUMN next_attempt_at TIMESTAMPTZ;

UPDATE outbox_events
SET
    status = CASE
        WHEN published_at IS NOT NULL THEN 'published'
        WHEN locked_until IS NOT NULL AND locked_until > NOW() THEN 'claimed'
        ELSE 'pending'
    END,
    next_attempt_at = NOW(),
    locked_until = CASE
        WHEN published_at IS NULL AND locked_until > NOW()
        THEN locked_until
        ELSE NULL
    END,
    locked_by = CASE
        WHEN published_at IS NULL AND locked_until > NOW()
        THEN locked_by
        ELSE NULL
    END;

ALTER TABLE outbox_events
    ALTER COLUMN status SET NOT NULL,
    ALTER COLUMN status SET DEFAULT 'pending',
    ALTER COLUMN next_attempt_at SET NOT NULL,
    ALTER COLUMN next_attempt_at SET DEFAULT NOW();

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_status
        CHECK (status IN ('pending', 'claimed', 'published', 'failed'));

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_claimed_state
        CHECK (
            (status = 'claimed' AND locked_until IS NOT NULL AND locked_by IS NOT NULL)
            OR
            (status <> 'claimed' AND locked_until IS NULL AND locked_by IS NULL)
        );

ALTER TABLE outbox_events
    ADD CONSTRAINT chk_outbox_published_state
        CHECK (
            (status = 'published' AND published_at IS NOT NULL)
            OR
            (status <> 'published' AND published_at IS NULL)
        );


-- +goose Down
ALTER TABLE outbox_events
    DROP CONSTRAINT IF EXISTS chk_outbox_published_state,
    DROP CONSTRAINT IF EXISTS chk_outbox_claimed_state,
    DROP CONSTRAINT IF EXISTS chk_outbox_status,
    DROP COLUMN IF EXISTS next_attempt_at,
    DROP COLUMN IF EXISTS status;
