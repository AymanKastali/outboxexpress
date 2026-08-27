-- +goose Up
CREATE SCHEMA notifications;

CREATE TABLE notifications.inbox (
    consumer      TEXT        NOT NULL,
    event_id      UUID        NOT NULL,
    event_type    TEXT        NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

CREATE INDEX inbox_processed_at_idx ON notifications.inbox (processed_at);

CREATE TABLE notifications.notifications (
    id          UUID        PRIMARY KEY,
    user_id     UUID        NOT NULL,
    recipient   TEXT        NOT NULL,
    kind        TEXT        NOT NULL,
    state       TEXT        NOT NULL DEFAULT 'pending',
    source_event_id UUID    NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at     TIMESTAMPTZ,

    CONSTRAINT notification_state_check
        CHECK (state IN ('pending', 'sent', 'failed'))
);

-- The consumer's own outbox: durable intents for the external call (spec §13).
CREATE TABLE notifications.outbox (
    id              BIGSERIAL     PRIMARY KEY,
    event_id        UUID          NOT NULL UNIQUE,
    aggregate_type  TEXT          NOT NULL,
    aggregate_id    TEXT          NOT NULL,
    event_type      TEXT          NOT NULL,
    schema_version  INTEGER       NOT NULL DEFAULT 1,
    payload         JSONB         NOT NULL,
    headers         JSONB         NOT NULL DEFAULT '{}',
    occurred_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    idempotency_key TEXT          NOT NULL,
    status          TEXT          NOT NULL DEFAULT 'pending',
    attempts        INTEGER       NOT NULL DEFAULT 0,
    available_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_error      TEXT,
    published_at    TIMESTAMPTZ,

    CONSTRAINT notif_outbox_status_check
        CHECK (status IN ('pending', 'published', 'failed'))
);

CREATE INDEX notif_outbox_pending_idx
    ON notifications.outbox (available_at, id)
    WHERE status = 'pending';

-- +goose Down
DROP TABLE notifications.outbox;
DROP TABLE notifications.notifications;
DROP TABLE notifications.inbox;
DROP SCHEMA notifications;
