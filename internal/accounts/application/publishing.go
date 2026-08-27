package application

import (
	"context"
	"errors"
	"time"

	"github.com/AymanKastali/outboxexpress/internal/platform/messaging"
)

// EventPublisher hands one message to the broker and returns only once the
// broker has durably acknowledged it (spec §10.1).
//
// The synchronous shape is the contract, not an implementation detail. §7:
// "Fire-and-forget publishing plus 'mark published' loses messages: the relay
// records success, the broker never durably had the message, and the row is gone
// from the pending set forever."
//
// Every error it returns wraps ErrTransient or ErrPermanent.
type EventPublisher interface {
	Publish(ctx context.Context, msg messaging.Message) error
}

// The two error classes of spec §12.1, and the most consequential distinction in
// the relay. §7 calls mistaking one for the other the most common relay bug in
// production: treating a broker outage as a per-message failure dead-letters an
// entire backlog, one row at a time, while everything looks like it is working.
//
// Every adapter wraps every error it returns in exactly one of these, and
// PublishPendingBatch branches on nothing else.
//
// A transient error never moves a row to 'failed', no matter how often it
// recurs: attempts drives backoff and nothing else (D8). RELAY_MAX_ATTEMPTS is
// an alert threshold, and whether a row is genuinely poison is a judgement a
// human makes with last_error in front of them.
//
// They live beside EventPublisher because they are its contract, not a separate
// concern that happens to be an error.
var (
	ErrTransient = errors.New("application: transient publish failure")
	ErrPermanent = errors.New("application: permanent publish failure")
)

// Schedule is how long a row waits after a transient failure, given the attempts
// it had already recorded before this one (spec §12.1).
//
// It is a port for the same reason Clock is: the arithmetic draws a random number,
// which is I/O against the machine. It is *only* a port — the formula itself lives
// in internal/platform/backoff, so that §12.2's consumer ladder can reuse it
// rather than hand-write the same overflow-prone doubling a second time.
//
// It belongs in this file rather than its own because it exists to answer one
// question — how long after a transient publish failure — and ErrTransient is
// what raises that question.
type Schedule interface {
	After(attempts int) time.Duration
}
