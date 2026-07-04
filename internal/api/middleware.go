package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"notifier/internal/observability"
)

const correlationIDHeader = "X-Request-ID"

// correlationID reads the client-supplied X-Request-ID or mints a new one,
// stores it in the request context, and echoes it on the response.
func correlationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestID := request.Header.Get(correlationIDHeader)
		if requestID == "" {
			requestID = uuid.NewString()
		}

		ctx := observability.WithCorrelationID(request.Context(), requestID)
		writer.Header().Set(correlationIDHeader, requestID)
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

// requestLogger emits one structured line per request with the
// context's correlation ID attached. chi's response wrapper is used so
// Flusher/Hijacker pass through (the WebSocket upgrade needs Hijacker).
func requestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			started := time.Now()
			wrapped := middleware.NewWrapResponseWriter(writer, request.ProtoMajor)

			next.ServeHTTP(wrapped, request)

			status := wrapped.Status()
			if status == 0 {
				status = http.StatusOK // handler returned without writing
			}
			observability.LoggerFrom(request.Context(), logger).Info("http request",
				slog.String("method", request.Method),
				slog.String("path", request.URL.Path),
				slog.Int("status", status),
				slog.Duration("duration", time.Since(started)),
			)
		})
	}
}

// recoverPanic converts handler panics into 500 responses and logs them
// instead of crashing the process.
func recoverPanic(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			defer func() {
				recovered := recover()
				if recovered == nil {
					return
				}
				if recovered == http.ErrAbortHandler {
					panic(recovered)
				}

				observability.LoggerFrom(request.Context(), logger).Error("panic recovered",
					slog.Any("panic", recovered),
					slog.String("method", request.Method),
					slog.String("path", request.URL.Path),
				)
				http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			}()

			next.ServeHTTP(writer, request)
		})
	}
}
