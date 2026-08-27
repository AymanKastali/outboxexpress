// Package backoff holds the retry arithmetic of spec §12.1 and the jitter that
// keeps processes which restarted together from falling into step.
//
// It is platform rather than application because the formula has no bounded
// context in it: §12.1's relay ladder and §12.2's consumer ladder are the same
// arithmetic with a different base, and the dependency rule (§6.1) would make a
// copy inside one context unreachable from the other. Each context declares its
// own one-method port over this — see application.Schedule.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Exponential is the schedule of spec §12.1:
//
//	backoff(n) = min(cap, base · 2^n) · random(0.5, 1.5)
//
// Jitter is a field rather than a call to Factor so that a test can pin it; a
// schedule whose every expectation is a range asserts almost nothing. Use
// NewExponential in production — a nil Jitter panics.
type Exponential struct {
	Base   time.Duration
	Cap    time.Duration
	Jitter func() float64
}

// NewExponential is the constructor process wiring uses.
func NewExponential(base, cap time.Duration) Exponential {
	return Exponential{Base: base, Cap: cap, Jitter: Factor}
}

// After returns how long to wait, given the attempts already recorded *before*
// this failure.
//
// n is the pre-failure count, so the first retry waits exactly Base — which is
// what RELAY_BACKOFF_BASE claims to mean. Reading n as the post-increment value
// would make the first wait 2·Base and the configured base a number that never
// appears.
func (e Exponential) After(attempts int) time.Duration {
	// Doubling toward the cap, rather than Base<<attempts clamped afterwards.
	// The shift overflows an int64 Duration well before attempt 63 for any
	// realistic Base, and an overflowed shift can wrap to a *small positive*
	// value — a backoff that gets shorter the longer the outage lasts, which is
	// the opposite of the property this function exists to provide.
	//
	// Saturating *before* the multiply rather than clamping after it is what
	// makes that provable for any Cap: delay < Cap/2 going into the doubling, so
	// delay*2 < Cap and cannot wrap. Clamping afterwards would only be safe for
	// a Cap below MaxInt64/2, which is a bound nothing enforces.
	//
	// A negative attempts needs no guard: `for range n` runs zero times.
	delay := min(e.Base, e.Cap)
	for range attempts {
		if delay >= e.Cap/2 {
			delay = e.Cap
			break
		}
		delay *= 2
	}
	return time.Duration(float64(delay) * e.Jitter())
}

// Factor returns a multiplier in [0.5, 1.5): random(0.5, 1.5) of spec §12.1.
//
// math/rand/v2's top-level functions are seeded randomly per process and are
// safe for concurrent use, so there is no seed to manage and no state to guard.
// The relay, the purger and every schedule share this one.
func Factor() float64 { return 0.5 + rand.Float64() }
