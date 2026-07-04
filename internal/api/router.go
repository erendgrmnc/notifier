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
	Templates      TemplateService
	// EventHub serves the /ws live status stream when set.
	EventHub *Hub

	// Observability endpoints; nil-safe for tests.
	Metrics        HTTPMetrics
	MetricsHandler http.Handler
	Readiness      ReadinessChecks
	// Gatherer + WorkerMetricsURL feed the dashboard's metrics summary;
	// LifetimeCounts adds restart-proof totals from the database.
	Gatherer         MetricsGatherer
	LifetimeCounts   LifetimeCounter
	WorkerMetricsURL string

	// Testing-dashboard dependencies; mounted only when DashboardEnabled.
	DashboardEnabled bool
	WorkerControl    WorkerControl
	Queues           QueueInspector
	ProviderStore    *mockprovider.Store
	// DefaultProviderURL is displayed as the effective target when no
	// runtime override is set.
	DefaultProviderURL string
}

// NewRouter builds the service router with the standard middleware chain:
// correlation ID → metrics → request logging → panic recovery → timeout.
func NewRouter(cfg RouterConfig) *chi.Mux {
	router := chi.NewRouter()

	router.Use(correlationID)
	if cfg.Metrics != nil {
		router.Use(httpMetrics(cfg.Metrics))
	}
	router.Use(requestLogger(cfg.Logger))
	router.Use(recoverPanic(cfg.Logger))
	// The per-request timeout is applied on the API group below, not
	// globally: /ws is deliberately long-lived.

	router.Get("/healthz", handleHealthz)
	if cfg.Readiness != nil {
		router.Get("/readyz", handleReadyz(cfg.Readiness))
	}
	if cfg.EventHub != nil {
		router.Get("/ws", cfg.EventHub.serveWS)
	}
	if cfg.MetricsHandler != nil {
		router.Method(http.MethodGet, "/metrics", cfg.MetricsHandler)
	}
	router.Get("/docs", handleSwaggerUI)

	// The contract is static; serve it in every mode.
	router.Get("/api/v1/openapi.yaml", handleOpenAPISpec)

	// A nil service builds an ops-only router (worker role): probes and
	// metrics without the notification API.
	if cfg.Notifications == nil {
		return router
	}

	notifications := &notificationHandler{notifications: cfg.Notifications, logger: cfg.Logger}
	dashboard := &dashboardHandler{
		workerControl:   cfg.WorkerControl,
		queues:          cfg.Queues,
		providerStore:   cfg.ProviderStore,
		defaultProvider: cfg.DefaultProviderURL,
		logger:          cfg.Logger,
	}

	router.Route("/api/v1", func(v1 chi.Router) {
		v1.Use(middleware.Timeout(cfg.RequestTimeout))
		v1.Post("/notifications", notifications.create)
		v1.Post("/notifications/batch", notifications.createBatch)
		v1.Get("/notifications", notifications.list)
		v1.Get("/notifications/{id}", notifications.get)
		v1.Post("/notifications/{id}/cancel", notifications.cancel)

		if cfg.Templates != nil {
			templates := &templateHandler{templates: cfg.Templates, logger: cfg.Logger}
			v1.Post("/templates", templates.create)
			v1.Get("/templates", templates.list)
			v1.Get("/templates/{name}", templates.get)
		}

		if cfg.DashboardEnabled {
			v1.Get("/queues", dashboard.getQueueDepths)
			v1.Get("/worker", dashboard.getWorkerState)
			v1.Put("/worker", dashboard.setWorkerState)
			v1.Get("/provider", dashboard.getProvider)
			v1.Put("/provider", dashboard.setProvider)
			v1.Get("/provider/messages", dashboard.getProviderMessages)
			if cfg.Gatherer != nil {
				summaryHandler := newMetricsSummaryHandler(cfg.Gatherer, cfg.LifetimeCounts, cfg.WorkerMetricsURL)
				v1.Get("/metrics/summary", summaryHandler.serve)
			}
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
