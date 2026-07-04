// Package observability provides structured logging and correlation-ID
// propagation shared by the HTTP and AMQP entry points.
package observability

import (
	"context"
	"log/slog"
)

type contextKey struct{ name string }

var correlationIDKey = contextKey{name: "correlation_id"}

// WithCorrelationID returns a context carrying the given correlation ID.
func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	return context.WithValue(ctx, correlationIDKey, correlationID)
}

// CorrelationIDFrom returns the correlation ID stored in ctx, or "" if none.
func CorrelationIDFrom(ctx context.Context) string {
	correlationID, _ := ctx.Value(correlationIDKey).(string)
	return correlationID
}

// LoggerFrom returns the base logger annotated with the context's
// correlation ID, so every log line in a request/message flow is joinable.
func LoggerFrom(ctx context.Context, base *slog.Logger) *slog.Logger {
	correlationID := CorrelationIDFrom(ctx)
	if correlationID == "" {
		return base
	}
	return base.With(slog.String("correlation_id", correlationID))
}
