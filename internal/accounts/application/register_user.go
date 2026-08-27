package application

import (
	"context"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

type RegisterUserCommand struct {
	Email       string
	DisplayName string
	Meta        Metadata
}

type RegisterUserResult struct {
	UserID uuid.UUID
}

// RegisterUser is the whole producing use case. Read it and notice what is
// missing: there is no outbox here. That is the point of spec §5 — the invariant
// "every state change emits its event" is structural, enforced by the unit of
// work, not a line a future call site has to remember to write.
type RegisterUser struct {
	uow    UnitOfWork
	clock  Clock
	ids    IDGen
	wakeup Wakeup
}

func NewRegisterUser(uow UnitOfWork, clock Clock, ids IDGen, wakeup Wakeup) *RegisterUser {
	return &RegisterUser{uow: uow, clock: clock, ids: ids, wakeup: wakeup}
}

func (uc *RegisterUser) Execute(ctx context.Context, cmd RegisterUserCommand) (RegisterUserResult, error) {
	id, err := uc.ids.New()
	if err != nil {
		return RegisterUserResult{}, err
	}

	user, err := domain.Register(id, cmd.Email, cmd.DisplayName, uc.clock.Now())
	if err != nil {
		return RegisterUserResult{}, err // pure domain, no I/O has happened yet
	}

	if err := uc.uow.Do(ctx, cmd.Meta, func(w Work) error {
		return w.Users.Save(ctx, user) // ErrEmailTaken surfaces here
	}); err != nil {
		return RegisterUserResult{}, err // <- the atomic moment
	}

	// Outside the transaction, best effort, error discarded: a failed wakeup
	// costs latency and nothing else, because the poll loop is authoritative.
	uc.wakeup.Notify(ctx)

	return RegisterUserResult{UserID: user.ID()}, nil
}
