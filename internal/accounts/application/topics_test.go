package application

import (
	"errors"
	"strings"
	"testing"
)

func TestTopics_ForRoutesOnAggregateType(t *testing.T) {
	topics := Topics{"User": "accounts.user.v1"}

	got, err := topics.For("User")
	if err != nil {
		t.Fatalf("For(User): %v", err)
	}
	if got != "accounts.user.v1" {
		t.Errorf("For(User) = %q, want %q", got, "accounts.user.v1")
	}
}

// Spec §9.2: "An unmapped type is a *permanent* error, so the branch is
// reachable." Permanent and not transient, because no amount of waiting adds a
// routing table entry.
func TestTopics_AnUnmappedAggregateTypeIsPermanent(t *testing.T) {
	topics := Topics{"User": "accounts.user.v1"}

	_, err := topics.For("Invoice")
	if !errors.Is(err, ErrPermanent) {
		t.Errorf("err = %v, want it to wrap ErrPermanent", err)
	}
	if !errors.Is(err, ErrUnmappedAggregateType) {
		t.Errorf("err = %v, want it to wrap ErrUnmappedAggregateType", err)
	}
	if !strings.Contains(err.Error(), "Invoice") {
		t.Errorf("err = %q, want it to name the type nobody mapped", err)
	}
}

// A table with the key present but empty is a misconfiguration, not a route: an
// empty topic name would reach the broker as a produce to "".
func TestTopics_AnEmptyTopicIsUnmapped(t *testing.T) {
	if _, err := (Topics{"User": ""}).For("User"); !errors.Is(err, ErrPermanent) {
		t.Errorf("err = %v, want it to wrap ErrPermanent", err)
	}
}
