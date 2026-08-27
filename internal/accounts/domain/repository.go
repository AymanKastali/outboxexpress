package domain

import "context"

// UserRepository is declared in the domain because in the Onion/DDD vocabulary
// the brief names, a Repository is a domain concept: it is the collection of
// users, spoken about the way a domain expert would (spec D5). An outbox is not,
// which is why the outbox port lives in the application layer instead.
//
// The method is Save, not Add or Insert: this is a persistence-oriented
// repository in Vernon's sense (IDDD ch. 12). A collection-oriented Add would
// promise that the aggregate is tracked from then on and its later mutations
// persisted implicitly, which is what an ORM with a dirty-check gives you. Go
// and pgx give no such thing, so the interface says plainly that the caller
// hands over an aggregate to be written. Insert would be worse still: it names
// the SQL verb an adapter happens to use, letting a storage detail set the
// domain's vocabulary.
//
// Save returns ErrEmailTaken when the address is already registered.
type UserRepository interface {
	Save(ctx context.Context, u *User) error
}
