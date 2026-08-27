// Package logging builds the one logger a process uses. JSON, because these
// logs are read by a machine first and a human second, and structured logs are
// how an outbox incident gets diagnosed from a row id.
package logging

import (
	"log/slog"
	"os"
)

// New takes an already-parsed level, so it has nothing to validate and no error
// to return. config.LoadAPI does the parsing, once, with slog's own parser.
func New(level slog.Level) *slog.Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
}
