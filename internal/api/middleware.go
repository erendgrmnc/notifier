package api

import (
	"log/slog"
	"net/http"
	"time"

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

// statusRecorder captures the response code for the request log line.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (recorder *statusRecorder) WriteHeader(code int) {
	recorder.status = code
	recorder.ResponseWriter.WriteHeader(code)
}

// requestLogger emits one structured line per request with the
// context's correlation ID attached.
func requestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			started := time.Now()
			recorder := &statusRecorder{ResponseWriter: writer, status: http.StatusOK}

			next.ServeHTTP(recorder, request)

			observability.LoggerFrom(request.Context(), logger).Info("http request",
				slog.String("method", request.Method),
				slog.String("path", request.URL.Path),
				slog.Int("status", recorder.status),
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
