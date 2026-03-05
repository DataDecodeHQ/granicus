package logging

import (
	"log/slog"
	"os"
)

// Init configures the default slog logger.
// serverMode=true uses JSON to stderr; false uses text to stderr.
func Init(serverMode bool) {
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if serverMode {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	} else {
		handler = slog.NewTextHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(handler))
}
