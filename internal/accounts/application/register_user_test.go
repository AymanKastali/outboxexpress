package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/AymanKastali/outboxexpress/internal/accounts/domain"
)

var (
	ucID  = uuid.MustParse("9f3c1e6a-4b2d-4f8a-9c11-77a2e0d3b5f1")
	ucNow = time.Date(2026, 8, 26, 10, 15, 30, 100_000_000, time.UTC)
)

func newUseCase(repo *fakeUserRepo) (*RegisterUser, *fakeUOW, *fakeWakeup) {
	uow := &fakeUOW{repo: repo}
	wake := &fakeWakeup{}
	uc := NewRegisterUser(uow, fixedClock{now: ucNow}, fixedIDs{next: ucID}, wake)
	return uc, uow, wake
}

func TestRegisterUser_CommitsThenWakesTheRelay(t *testing.T) {
	repo := &fakeUserRepo{}
	uc, uow, wake := newUseCase(repo)

	got, err := uc.Execute(context.Background(), RegisterUserCommand{
		Email:       "ada@example.com",
		DisplayName: "Ada Lovelace",
		Meta:        Metadata{CorrelationID: "corr-1", Traceparent: "tp-1"},
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got.UserID != ucID {
		t.Errorf("UserID = %s, want %s", got.UserID, ucID)
	}
	if uow.calls != 1 {
		t.Errorf("uow.calls = %d, want 1", uow.calls)
	}
	if !uow.committed {
		t.Error("transaction did not commit")
	}
	if uow.lastMeta.CorrelationID != "corr-1" || uow.lastMeta.Traceparent != "tp-1" {
		t.Errorf("metadata not passed to the unit of work: %+v", uow.lastMeta)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("saved %d users, want 1", len(repo.saved))
	}
	if repo.saved[0].Email().String() != "ada@example.com" {
		t.Errorf("Email = %q", repo.saved[0].Email())
	}
	if !repo.saved[0].CreatedAt().Equal(ucNow) {
		t.Errorf("CreatedAt = %v, want the injected clock's time %v", repo.saved[0].CreatedAt(), ucNow)
	}
	if wake.calls != 1 {
		t.Errorf("wakeup.calls = %d, want 1", wake.calls)
	}
}

func TestRegisterUser_DoesNotTouchTheDatabaseOnInvalidInput(t *testing.T) {
	repo := &fakeUserRepo{}
	uc, uow, wake := newUseCase(repo)

	_, err := uc.Execute(context.Background(), RegisterUserCommand{
		Email:       "not-an-address",
		DisplayName: "Ada Lovelace",
	})
	if !errors.Is(err, domain.ErrInvalidEmail) {
		t.Fatalf("err = %v, want ErrInvalidEmail", err)
	}
	if uow.calls != 0 {
		t.Errorf("uow.calls = %d, want 0 — validation happens before the transaction opens", uow.calls)
	}
	if wake.calls != 0 {
		t.Errorf("wakeup.calls = %d, want 0", wake.calls)
	}
}

func TestRegisterUser_SurfacesEmailTaken(t *testing.T) {
	repo := &fakeUserRepo{err: domain.ErrEmailTaken}
	uc, uow, wake := newUseCase(repo)

	_, err := uc.Execute(context.Background(), RegisterUserCommand{
		Email:       "ada@example.com",
		DisplayName: "Ada Lovelace",
	})
	if !errors.Is(err, domain.ErrEmailTaken) {
		t.Fatalf("err = %v, want ErrEmailTaken", err)
	}
	if uow.committed {
		t.Error("the transaction must not commit when the save fails")
	}
	if wake.calls != 0 {
		t.Error("no event was written, so there is nothing to wake the relay for")
	}
}

func TestRegisterUser_DoesNotWakeTheRelayWhenTheCommitFails(t *testing.T) {
	repo := &fakeUserRepo{}
	uow := &fakeUOW{repo: repo, commitErr: errors.New("connection reset")}
	wake := &fakeWakeup{}
	uc := NewRegisterUser(uow, fixedClock{now: ucNow}, fixedIDs{next: ucID}, wake)

	if _, err := uc.Execute(context.Background(), RegisterUserCommand{
		Email:       "ada@example.com",
		DisplayName: "Ada Lovelace",
	}); err == nil {
		t.Fatal("expected the commit error to surface")
	}
	if wake.calls != 0 {
		t.Error("waking the relay for an uncommitted transaction advertises an event that does not exist")
	}
}

func TestRegisterUser_FailsBeforeAnyWorkWhenIDGenerationFails(t *testing.T) {
	repo := &fakeUserRepo{}
	uow := &fakeUOW{repo: repo}
	uc := NewRegisterUser(uow, fixedClock{now: ucNow}, fixedIDs{err: errIDs}, &fakeWakeup{})

	if _, err := uc.Execute(context.Background(), RegisterUserCommand{
		Email:       "ada@example.com",
		DisplayName: "Ada Lovelace",
	}); !errors.Is(err, errIDs) {
		t.Fatalf("err = %v, want errIDs", err)
	}
	if uow.calls != 0 {
		t.Errorf("uow.calls = %d, want 0", uow.calls)
	}
}
