package clock

import (
	"testing"
	"time"
)

func TestSystem_ReturnsUTC(t *testing.T) {
	got := System{}.Now()
	if got.Location() != time.UTC {
		t.Fatalf("location = %v, want UTC", got.Location())
	}
}
