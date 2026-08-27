-- +goose Up

-- failed_rows on the pass line (§13.3) is read every pass. Partial, because
-- parked rows are rare by construction — if this index is ever large, that is
-- itself the alert.
CREATE INDEX outbox_failed_idx
    ON accounts.outbox (id)
    WHERE status = 'failed';

-- The purge's page (§13.2): published rows in id order, oldest first. Without
-- it, a purge that finds nothing eligible still pays for the whole table.
CREATE INDEX outbox_purge_idx
    ON accounts.outbox (published_at, id)
    WHERE status = 'published';

-- +goose Down
DROP INDEX accounts.outbox_purge_idx;
DROP INDEX accounts.outbox_failed_idx;
