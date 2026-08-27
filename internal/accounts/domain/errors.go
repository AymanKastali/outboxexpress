package domain

import "errors"

// The domain's vocabulary of refusal. These are sentinels so that every outer
// layer can map them — the HTTP layer to a status code, a test to an assertion —
// without matching on message text.
var (
	ErrInvalidEmail       = errors.New("accounts: email is not a valid address")
	ErrInvalidDisplayName = errors.New("accounts: display name is empty or too long")
	ErrInvalidID          = errors.New("accounts: user id must not be the nil UUID")

	// ErrEmailTaken is a domain rule the database enforces: the uniqueness of an
	// email is a fact about the whole collection of users, which no in-memory
	// check can establish under concurrency. The repository translates the
	// unique-violation into this error; the rule still belongs to the domain.
	ErrEmailTaken = errors.New("accounts: email is already registered")
)
