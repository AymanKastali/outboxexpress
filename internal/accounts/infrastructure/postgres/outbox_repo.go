// Package postgres is the accounts context's persistence layer: repositories,
// and the unit of work that makes the outbox insert structural.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/AymanKastali/outboxexpress/internal/accounts/application"
	platformpg "github.com/AymanKastali/outboxexpress/internal/platform/postgres"
)

// status, attempts and available_at are left to their column defaults rather
// than written here: the relay owns that bookkeeping, and a producer that set
// them would be making a decision that is not its to make.
const insertOutbox = `
INSERT INTO accounts.outbox
    (event_id, aggregate_type, aggregate_id, event_type, schema_version,
     payload, headers, occurred_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

type OutboxRepository struct {
	q platformpg.Queryer
}

func NewOutboxRepository(q platformpg.Queryer) *OutboxRepository {
	return &OutboxRepository{q: q}
}

// Append writes every envelope in one round trip. A pgx.Batch rather than a loop
// of Exec calls because these inserts sit inside the business transaction, and a
// transaction that is open for N network round trips holds its locks — and the
// xmin horizon — N times longer than it needs to (spec §11.1 rule 4).
func (r *OutboxRepository) Append(ctx context.Context, envelopes []application.Envelope) error {
	if len(envelopes) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, e := range envelopes {
		batch.Queue(insertOutbox,
			e.EventID,
			e.AggregateType,
			e.AggregateID,
			e.EventType,
			e.SchemaVersion,
			e.Payload,
			e.Headers,
			e.OccurredAt,
		)
	}
	results := r.q.SendBatch(ctx, batch)
	for i := range envelopes {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("postgres: append outbox row %d (event_id %s): %w",
				i, envelopes[i].EventID, err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("postgres: close outbox batch: %w", err)
	}
	return nil
}

var _ application.OutboxAppender = (*OutboxRepository)(nil)
