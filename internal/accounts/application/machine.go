package application

import (
	"time"

	"github.com/google/uuid"
)

// Clock and IDGen exist so that a use case is deterministic under test. Reading
// the wall clock or generating a UUID is I/O against the machine.
//
// Now must return UTC. Normalising is this layer's job, not the domain's: an
// aggregate handed a timestamp stores that instant unchanged, so whatever the
// clock returns is what gets written and published. A fake clock in a test owes
// the same guarantee as clock.System.
type Clock interface {
	Now() time.Time
}

type IDGen interface {
	New() (uuid.UUID, error)
}
