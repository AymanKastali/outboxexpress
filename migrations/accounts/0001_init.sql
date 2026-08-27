-- +goose Up
CREATE SCHEMA accounts;

CREATE TABLE accounts.users (
    id            UUID        PRIMARY KEY,
    email         TEXT        NOT NULL,
    display_name  TEXT        NOT NULL,
    version       INTEGER     NOT NULL DEFAULT 1,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Named, not inline: the repository translates this constraint's violation
    -- into domain.ErrEmailTaken by matching the name, so the name is part of
    -- the contract between the schema and the adapter. "users_email_key" is
    -- what PostgreSQL generates for an inline UNIQUE anyway, so writing it out
    -- changes nothing except who is on record as having chosen it.
    CONSTRAINT users_email_key UNIQUE (email)
);

CREATE TABLE accounts.outbox (
    -- identity and ordering
    id              BIGSERIAL     PRIMARY KEY,
    event_id        UUID          NOT NULL UNIQUE,

    -- routing
    aggregate_type  TEXT          NOT NULL,
    aggregate_id    TEXT          NOT NULL,
    event_type      TEXT          NOT NULL,
    schema_version  INTEGER       NOT NULL DEFAULT 1,

    -- content
    payload         JSONB         NOT NULL,
    headers         JSONB         NOT NULL DEFAULT '{}',
    occurred_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),

    -- relay bookkeeping
    status          TEXT          NOT NULL DEFAULT 'pending',
    attempts        INTEGER       NOT NULL DEFAULT 0,
    available_at    TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_error      TEXT,
    published_at    TIMESTAMPTZ,

    CONSTRAINT outbox_status_check
        CHECK (status IN ('pending', 'published', 'failed'))
);

-- The relay's only access path: pending rows, in id order, that are due.
CREATE INDEX outbox_pending_idx
    ON accounts.outbox (available_at, id)
    WHERE status = 'pending';

-- +goose Down
DROP TABLE accounts.outbox;
DROP TABLE accounts.users;
DROP SCHEMA accounts;
