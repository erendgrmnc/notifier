package observability

import (
	"io"
	"log/slog"
)

// NewLogger builds the service-wide JSON logger at the given level.
// Unknown level strings fall back to info.
func NewLogger(sink io.Writer, level string) *slog.Logger {
	var slogLevel slog.Level
	switch level {
	case "debug":
		slogLevel = slog.LevelDebug
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: slogLevel})
	return slog.New(handler)
}
