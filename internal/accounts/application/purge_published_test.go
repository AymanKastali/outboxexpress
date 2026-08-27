package application

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Spec §13.2: "The loop repeats until fewer than $2 rows are removed, so no
// purge takes a long lock or a long transaction."
func TestPurgePublished_RepeatsUntilAShortPage(t *testing.T) {
	purger := &fakePurger{returns: []int{1000, 1000, 240}}
	uc := NewPurgePublished(purger, 24*time.Hour, 1000)

	res, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Deleted != 2240 {
		t.Errorf("deleted = %d, want 2240", res.Deleted)
	}
	if res.Passes != 3 {
		t.Errorf("passes = %d, want 3", res.Passes)
	}
	if len(purger.calls) != 3 {
		t.Errorf("calls = %v, want three bounded deletes", purger.calls)
	}
	for i, limit := range purger.calls {
		if limit != 1000 {
			t.Errorf("call %d used limit %d, want 1000 — an unbounded delete is the "+
				"long lock §13.2 exists to avoid", i, limit)
		}
	}
}

// Nothing to do is the common case, and it must cost one statement, not a loop.
func TestPurgePublished_NothingToDeleteIsOnePass(t *testing.T) {
	purger := &fakePurger{returns: []int{0}}
	uc := NewPurgePublished(purger, 24*time.Hour, 1000)

	res, err := uc.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if res.Deleted != 0 || res.Passes != 1 {
		t.Errorf("result = %+v, want zero deleted in one pass", res)
	}
}

// A shutdown mid-purge must stop between deletes, not after all of them. A purge
// is interruptible by design: the rows it did not delete are still published and
// still purgeable next time.
func TestPurgePublished_StopsWhenTheContextEnds(t *testing.T) {
	purger := &fakePurger{returns: []int{1000, 1000, 1000, 1000}}
	uc := NewPurgePublished(purger, 24*time.Hour, 1000)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := uc.Execute(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
	if len(purger.calls) != 0 {
		t.Errorf("issued %d deletes after cancellation, want 0", len(purger.calls))
	}
	if res.Deleted != 0 {
		t.Errorf("deleted = %d, want 0", res.Deleted)
	}
}

// A failure returns what was already deleted, because those rows are gone and
// the log line should say so rather than reporting zero. The fake clears a full
// page and then fails, so there is something for the result to be wrong about.
func TestPurgePublished_ReportsWhatItManagedBeforeFailing(t *testing.T) {
	purger := &fakePurger{returns: []int{1000}, err: errors.New("connection reset")}
	uc := NewPurgePublished(purger, 24*time.Hour, 1000)

	res, err := uc.Execute(context.Background())
	if err == nil {
		t.Fatal("Execute returned nil on a failing delete")
	}
	if res.Deleted != 1000 {
		t.Errorf("deleted = %d, want 1000 — those rows are gone whatever happened next",
			res.Deleted)
	}
	if res.Passes != 1 {
		t.Errorf("passes = %d, want 1 — the failing page did not complete", res.Passes)
	}
}

// fakePurger answers with a script of counts, so a test can describe a table
// that takes three bounded deletes to clear.
type fakePurger struct {
	returns []int // the count each call answers with, in order
	err     error // returned once the script is exhausted, if set
	calls   []int // the limit each call was given
}

func (f *fakePurger) PurgePublished(_ context.Context, _ time.Duration, limit int) (int, error) {
	f.calls = append(f.calls, limit)
	if n := len(f.calls) - 1; n < len(f.returns) {
		return f.returns[n], nil
	}
	// Past the end of the script: fail if the test asked for a failure, otherwise
	// report an empty page, which is what a cleared table looks like.
	return 0, f.err
}
