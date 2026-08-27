-- +goose Up

-- failed_rows on the pass line (§13.3) is read every pass. Partial, because
-- parked rows are rare by construction — if this index is ever large, that is
-- itself the alert.
CREATE INDEX outbox_failed_idx
    ON accounts.outbox (id)
    WHERE status = 'failed';

-- The purge's page (§13.2). Leading on published_at because that is the purge's
-- predicate; the page itself is ORDER BY id, which this index does not provide,
-- and the planner is right to walk the primary key for a page that will be found
-- early. What this index is for is the *empty* case — a fresh deploy, a quiet
-- night, a retention that was just raised — where a primary-key walk would read
-- the whole table to conclude that nothing is eligible, once a minute, forever.
CREATE INDEX outbox_purge_idx
    ON accounts.outbox (published_at, id)
    WHERE status = 'published';

-- +goose Down
DROP INDEX accounts.outbox_purge_idx;
DROP INDEX accounts.outbox_failed_idx;
