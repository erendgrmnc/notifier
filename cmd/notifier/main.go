// Command notifier runs the notification service. ROLE selects which
// components start: api, worker, or all (default).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"notifier/internal/api"
	"notifier/internal/config"
	"notifier/internal/delivery"
	"notifier/internal/mockprovider"
	"notifier/internal/observability"
	"notifier/internal/queue/rabbit"
	"notifier/internal/scheduler"
	"notifier/internal/service"
	"notifier/internal/storage/postgres"
	"notifier/internal/worker"
)

const requestTimeout = 30 * time.Second

// realClock is the production Clock; tests inject fixed clocks instead.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// queueDepthPollInterval is how often the queue-depth gauge samples.
const queueDepthPollInterval = 5 * time.Second

// pollQueueDepths keeps the queue_depth gauge current until ctx ends.
func pollQueueDepths(ctx context.Context, inspector *rabbit.Inspector, metrics *observability.Metrics, logger *slog.Logger) {
	ticker := time.NewTicker(queueDepthPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depths, err := inspector.QueueDepths(ctx)
			if err != nil {
				logger.Warn("queue depth poll failed", slog.Any("error", err))
				continue
			}
			for _, depth := range depths {
				metrics.SetQueueDepth(depth.Name, depth.Ready)
			}
		}
	}
}

// newSender selects the delivery provider: the real webhook sender when
// PROVIDER_URL is configured, otherwise the logging simulator.
func newSender(cfg config.Config, logger *slog.Logger) worker.Sender {
	if cfg.ProviderURL == "" {
		logger.Warn("PROVIDER_URL not set; deliveries are simulated")
		return delivery.NewLogSender(logger)
	}
	return delivery.NewWebhookSender(cfg.ProviderURL, cfg.ProviderTimeout)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := observability.NewLogger(os.Stdout, cfg.LogLevel)
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("starting", slog.String("role", string(cfg.Role)), slog.Int("http_port", cfg.HTTPPort))

	migrationsApplied, err := postgres.Migrate(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	if migrationsApplied {
		logger.Info("migrations applied")
	} else {
		logger.Info("database schema up to date")
	}

	pool, err := postgres.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()

	rabbitConn, err := rabbit.Connect(cfg.RabbitURL)
	if err != nil {
		return fmt.Errorf("connect rabbitmq: %w", err)
	}
	defer rabbitConn.Close()

	if err := rabbit.DeclareTopology(rabbitConn); err != nil {
		return fmt.Errorf("declare topology: %w", err)
	}

	repository := postgres.NewNotificationRepository(pool)

	publisher, err := rabbit.NewPublisher(rabbitConn)
	if err != nil {
		return fmt.Errorf("create publisher: %w", err)
	}
	defer publisher.Close()

	metrics := observability.NewMetrics()
	inspector := rabbit.NewInspector(rabbitConn)
	readiness := api.ReadinessChecks{
		"postgres": func(checkCtx context.Context) error { return pool.Ping(checkCtx) },
		"rabbitmq": func(context.Context) error {
			if rabbitConn.IsClosed() {
				return errors.New("connection closed")
			}
			return nil
		},
	}

	runsAPI := cfg.Role == config.RoleAPI || cfg.Role == config.RoleAll
	runsWorker := cfg.Role == config.RoleWorker || cfg.Role == config.RoleAll

	var componentGroup sync.WaitGroup
	fatalErr := make(chan error, 2)

	// Queue-depth gauge poller: owned here, stopped by ctx, joined below.
	componentGroup.Add(1)
	go func() {
		defer componentGroup.Done()
		pollQueueDepths(ctx, inspector, metrics, logger)
	}()

	var httpServer *http.Server
	if runsAPI {
		templateRepository := postgres.NewTemplateRepository(pool)
		notifications := service.NewNotificationService(repository, repository, templateRepository, publisher, realClock{}, logger, metrics)
		templates := service.NewTemplateService(templateRepository, realClock{})

		// Live status stream: hub owns the clients; the events consumer
		// feeds it from the fanout exchange (works across the api/worker
		// process split).
		eventHub := api.NewHub(logger)
		componentGroup.Add(1)
		go func() {
			defer componentGroup.Done()
			eventHub.Run(ctx)
		}()
		componentGroup.Add(1)
		go func() {
			defer componentGroup.Done()
			if err := rabbit.ConsumeEvents(ctx, rabbitConn, logger, eventHub.BroadcastRaw); err != nil {
				logger.Warn("event stream unavailable", slog.Any("error", err))
			}
		}()

		router := api.NewRouter(api.RouterConfig{
			Logger:           logger,
			RequestTimeout:   requestTimeout,
			Notifications:    notifications,
			Templates:        templates,
			EventHub:         eventHub,
			Metrics:          metrics,
			MetricsHandler:   metrics.Handler(),
			Readiness:        readiness,
			Gatherer:         metrics,
			LifetimeCounts:   repository,
			WorkerMetricsURL: cfg.WorkerMetricsURL,
			DashboardEnabled: cfg.DashboardEnabled,
			WorkerControl:    repository,
			Queues:           inspector,
			ProviderStore:    mockprovider.NewStore(),
		})
		if cfg.DashboardEnabled {
			logger.Info("testing dashboard enabled", slog.String("path", "/dashboard"))
		}
		httpServer = &http.Server{Addr: fmt.Sprintf(":%d", cfg.HTTPPort), Handler: router}

		componentGroup.Add(1)
		go func() {
			defer componentGroup.Done()
			if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
				fatalErr <- fmt.Errorf("http server: %w", err)
			}
		}()
		logger.Info("api listening", slog.Int("port", cfg.HTTPPort))

		// The scheduler runs with the api role: it fires scheduled
		// notifications and recovers rows whose publish was lost.
		dueScheduler := scheduler.New(repository, publisher, realClock{}, logger, cfg.SchedulerPollInterval, cfg.StalePendingAfter)
		componentGroup.Add(1)
		go func() {
			defer componentGroup.Done()
			dueScheduler.Run(ctx)
		}()
	}

	if runsWorker {
		queueWorker := worker.New(repository, newSender(cfg, logger), publisher, repository, realClock{}, logger, metrics,
			cfg.MaxDeliveryAttempts, cfg.RateLimitPerChannel, cfg.WorkerConcurrency)

		componentGroup.Add(1)
		go func() {
			defer componentGroup.Done()
			if err := queueWorker.Run(ctx, rabbitConn, cfg.WorkerPrefetch); err != nil {
				fatalErr <- fmt.Errorf("worker: %w", err)
			}
		}()

		// A worker-only process still serves /healthz, /readyz, /metrics
		// so it is probeable and scrapeable.
		if !runsAPI {
			opsRouter := api.NewRouter(api.RouterConfig{
				Logger:         logger,
				RequestTimeout: requestTimeout,
				Metrics:        metrics,
				MetricsHandler: metrics.Handler(),
				Readiness:      readiness,
			})
			httpServer = &http.Server{Addr: fmt.Sprintf(":%d", cfg.HTTPPort), Handler: opsRouter}

			componentGroup.Add(1)
			go func() {
				defer componentGroup.Done()
				if err := httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
					fatalErr <- fmt.Errorf("ops server: %w", err)
				}
			}()
			logger.Info("ops endpoints listening", slog.Int("port", cfg.HTTPPort))
		}
	}

	select {
	case <-ctx.Done():
	case err := <-fatalErr:
		return err
	}

	// Shutdown order: stop HTTP intake first; consumers are already
	// draining via the cancelled context; then join everything.
	if httpServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown http server: %w", err)
		}
	}
	componentGroup.Wait()

	logger.Info("shutdown complete", slog.String("role", string(cfg.Role)))
	return nil
}
