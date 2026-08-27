package backoff_test

import (
	"testing"
	"time"

	"github.com/AymanKastali/outboxexpress/internal/platform/backoff"
)

func TestExponential_After(t *testing.T) {
	// backoff(n) = min(cap, base · 2^n) · random(0.5, 1.5) — spec §12.1, with n
	// the attempts already recorded *before* this failure, so the first retry
	// waits exactly Base. That is what makes RELAY_BACKOFF_BASE mean what its
	// name says.
	schedule := backoff.Exponential{
		Base:   time.Second,
		Cap:    5 * time.Minute,
		Jitter: fixed(1),
	}

	tests := []struct {
		attempts int
		want     time.Duration
	}{
		{attempts: 0, want: 1 * time.Second},
		{attempts: 1, want: 2 * time.Second},
		{attempts: 2, want: 4 * time.Second},
		{attempts: 8, want: 256 * time.Second},
		{attempts: 9, want: 5 * time.Minute}, // 512s > cap
		{attempts: 20, want: 5 * time.Minute},
		// An overflowing shift is the failure mode that matters: 1s << 62 wraps,
		// and a wrapped Duration can be small and positive — a backoff that gets
		// *shorter* the longer Kafka is down.
		{attempts: 62, want: 5 * time.Minute},
		{attempts: 1000, want: 5 * time.Minute},
		// attempts can only be >= 0 in the column, but a caller passing -1 must
		// not get a negative wait, which would be a busy loop. No guard is
		// needed for this: `for range n` with a negative n runs zero times.
		{attempts: -1, want: 1 * time.Second},
	}
	for _, tc := range tests {
		if got := schedule.After(tc.attempts); got != tc.want {
			t.Errorf("After(%d) = %v, want %v", tc.attempts, got, tc.want)
		}
	}
}

func TestExponential_AppliesTheJitterFactor(t *testing.T) {
	schedule := backoff.Exponential{
		Base:   10 * time.Second,
		Cap:    time.Minute,
		Jitter: fixed(0.5),
	}
	if got, want := schedule.After(0), 5*time.Second; got != want {
		t.Errorf("After(0) with factor 0.5 = %v, want %v", got, want)
	}
}

// NewExponential is what production calls, so the real Factor has to be wired in
// by it — a nil Jitter would panic on the first transient failure, in the
// process, at the worst possible moment.
func TestNewExponential_WiresTheRealJitter(t *testing.T) {
	schedule := backoff.NewExponential(time.Second, time.Minute)
	got := schedule.After(0)
	if got < 500*time.Millisecond || got >= 1500*time.Millisecond {
		t.Errorf("After(0) = %v, want it jittered within [0.5s, 1.5s)", got)
	}
}

// The range is the contract: a factor of 0 would collapse a backoff to nothing
// and turn the relay's retry into a spin, and a factor above 1.5 would stretch
// the cap past the ceiling spec §12.1 sets.
func TestFactor_StaysInRange(t *testing.T) {
	lo, hi := 2.0, 0.0
	for range 10_000 {
		f := backoff.Factor()
		if f < 0.5 || f >= 1.5 {
			t.Fatalf("Factor() = %v, want [0.5, 1.5)", f)
		}
		lo, hi = min(lo, f), max(hi, f)
	}
	// 10,000 draws from a uniform [0.5, 1.5) that never left the middle would
	// mean it is not uniform — the failure a range check alone cannot see.
	if lo > 0.55 || hi < 1.45 {
		t.Errorf("10000 draws spanned only [%v, %v]; that is not uniform over [0.5, 1.5)", lo, hi)
	}
}

// fixed is the factor a test wants: none. The real Factor multiplies by
// something in [0.5, 1.5), which would make every expectation a range.
func fixed(f float64) func() float64 { return func() float64 { return f } }
