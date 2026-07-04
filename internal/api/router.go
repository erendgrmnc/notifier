// Package api wires the HTTP transport: router, middleware, handlers.
package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// RouterConfig carries the dependencies the HTTP layer needs.
type RouterConfig struct {
	Logger         *slog.Logger
	RequestTimeout time.Duration
}

// NewRouter builds the service router with the standard middleware chain:
// correlation ID → request logging → panic recovery → per-request timeout.
func NewRouter(cfg RouterConfig) *chi.Mux {
	router := chi.NewRouter()

	router.Use(correlationID)
	router.Use(requestLogger(cfg.Logger))
	router.Use(recoverPanic(cfg.Logger))
	router.Use(middleware.Timeout(cfg.RequestTimeout))

	router.Get("/healthz", handleHealthz)

	return router
}

// handleHealthz reports process liveness only; dependency readiness
// belongs to /readyz.
func handleHealthz(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(http.StatusOK)
}
