package ids

import "testing"

func TestUUIDv7_IsVersion7AndMonotonicallyOrdered(t *testing.T) {
	g := UUIDv7{}
	prev, err := g.New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := prev.Version(); got != 7 {
		t.Fatalf("version = %d, want 7", got)
	}
	for i := 0; i < 100; i++ {
		next, err := g.New()
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if next.String() <= prev.String() {
			t.Fatalf("uuid %s not sorted after %s", next, prev)
		}
		prev = next
	}
}
