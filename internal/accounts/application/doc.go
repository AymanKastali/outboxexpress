// Package application holds the accounts use cases and the technical ports they
// need. It imports the domain and nothing concrete: no SQL, no HTTP, no Kafka,
// no logger (spec §6.1).
//
// Ports live next to what they are about, not in a file called ports. A reader
// looking for the outbox repository finds it in outbox.go beside PendingMessage;
// the envelope factory sits beside the envelope it produces and the only
// implementation of it; the wakeup is declared in register_user.go because that
// use case is its only caller. What is left in machine.go is the residue that
// genuinely belongs nowhere: the clock and the id generator, which every layer
// needs and no layer owns.
package application
