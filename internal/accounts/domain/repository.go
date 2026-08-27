package domain

import "context"

// UserRepository is declared in the domain because in the Onion/DDD vocabulary
// the brief names, a Repository is a domain concept: it is the collection of
// users, spoken about the way a domain expert would (spec D5). An outbox is not,
// which is why the outbox port lives in the application layer instead.
//
// Insert returns ErrEmailTaken when the address is already registered.
type UserRepository interface {
	Insert(ctx context.Context, u *User) error
}
