// Package ids generates identifiers. UUIDv7 is used everywhere an identifier is
// stored, because its leading timestamp gives B-tree index locality: v4 keys
// scatter inserts across the whole index, and the outbox is an insert-heavy
// table (spec §4, D10).
package ids

import "github.com/google/uuid"

// UUIDv7 is the generator, and the only one this package ships. A test that
// needs predictable identifiers declares its own generator in its own
// _test.go — see the seqIDs type in Task 7 and staticIDs in Task 12.
type UUIDv7 struct{}

// NewV7 already returns uuid.Nil alongside its error, so there is nothing to
// translate — this method exists only to name the port.
func (UUIDv7) New() (uuid.UUID, error) { return uuid.NewV7() }
