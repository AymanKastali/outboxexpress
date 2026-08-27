// Package clock exists so that no code outside it calls time.Now. A use case
// that reads the wall clock directly cannot be tested without sleeping, and a
// test suite that sleeps cannot prove an invariant (spec §15).
package clock

import "time"

// System is the real clock, and the only clock this package ships. It returns
// UTC because every timestamp in this system is stored and published in UTC, and
// converting once at the source is cheaper than remembering to convert at each
// use.
//
// A test that needs time to stand still declares its own two-line fake next to
// the test that needs it. Shipping a controllable clock from here would put code
// in the binary that only tests can reach.
type System struct{}

func (System) Now() time.Time { return time.Now().UTC() }
