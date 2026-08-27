package application

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

// fakeUOW runs fn immediately and records what happened. It models the two
// outcomes the use case can distinguish: fn failed, or the transaction
// committed.
type fakeUOW struct {
	calls     int
	committed bool
	lastMeta  Metadata
	repo      *fakeUserRepo
	commitErr error
}

func (f *fakeUOW) Do(ctx context.Context, meta Metadata, fn func(Work) error) error {
	f.calls++
	f.lastMeta = meta
	if err := fn(Work{Users: f.repo}); err != nil {
		return err
	}
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committed = true
	return nil
}

type fakeUserRepo struct {
	inserted []*domain.User
	err      error
}

func (f *fakeUserRepo) Insert(ctx context.Context, u *domain.User) error {
	if f.err != nil {
		return f.err
	}
	f.inserted = append(f.inserted, u)
	return nil
}

type fakeWakeup struct{ calls int }

func (f *fakeWakeup) Notify(ctx context.Context) { f.calls++ }

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

type fixedIDs struct {
	next uuid.UUID
	err  error
}

func (f fixedIDs) New() (uuid.UUID, error) {
	if f.err != nil {
		return uuid.Nil, f.err
	}
	return f.next, nil
}

var errIDs = errors.New("no entropy")
