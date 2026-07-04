// Package api wires the HTTP transport: router, middleware, handlers.
package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"notifier/internal/mockprovider"
)

// RouterConfig carries the dependencies the HTTP layer needs.
type RouterConfig struct {
	Logger         *slog.Logger
	RequestTimeout time.Duration
	Notifications  NotificationService

	// Testing-dashboard dependencies; mounted only when DashboardEnabled.
	DashboardEnabled bool
	WorkerControl    WorkerControl
	Queues           QueueInspector
	ProviderStore    *mockprovider.Store
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
	router.Get("/docs", handleSwaggerUI)

	notifications := &notificationHandler{notifications: cfg.Notifications, logger: cfg.Logger}
	dashboard := &dashboardHandler{
		workerControl: cfg.WorkerControl,
		queues:        cfg.Queues,
		providerStore: cfg.ProviderStore,
		logger:        cfg.Logger,
	}

	router.Route("/api/v1", func(v1 chi.Router) {
		v1.Get("/openapi.yaml", handleOpenAPISpec)
		v1.Post("/notifications", notifications.create)
		v1.Post("/notifications/batch", notifications.createBatch)
		v1.Get("/notifications", notifications.list)
		v1.Get("/notifications/{id}", notifications.get)
		v1.Post("/notifications/{id}/cancel", notifications.cancel)

		if cfg.DashboardEnabled {
			v1.Get("/queues", dashboard.getQueueDepths)
			v1.Get("/worker", dashboard.getWorkerState)
			v1.Put("/worker", dashboard.setWorkerState)
			v1.Get("/provider/messages", dashboard.getProviderMessages)
		}
	})

	if cfg.DashboardEnabled {
		router.Get("/dashboard", handleDashboard)
		router.Post("/provider/messages", cfg.ProviderStore.Receive)
	}

	return router
}

// handleHealthz reports process liveness only; dependency readiness
// belongs to /readyz.
func handleHealthz(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(http.StatusOK)
}
