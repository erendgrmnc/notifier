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
	"syscall"
	"time"

	"notifier/internal/api"
	"notifier/internal/config"
	"notifier/internal/observability"
)

const requestTimeout = 30 * time.Second

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

	switch cfg.Role {
	case config.RoleAPI, config.RoleAll:
		return runAPI(ctx, cfg, logger)
	case config.RoleWorker:
		// Worker components arrive with the queue integration; block
		// until shutdown so the role is wired end to end already.
		<-ctx.Done()
		logger.Info("shutdown complete", slog.String("role", string(cfg.Role)))
		return nil
	default:
		return fmt.Errorf("unhandled role %q", cfg.Role)
	}
}

// runAPI serves HTTP until the context is cancelled, then drains
// in-flight requests within the configured shutdown timeout.
func runAPI(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	router := api.NewRouter(api.RouterConfig{
		Logger:         logger,
		RequestTimeout: requestTimeout,
	})

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler: router,
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("http server: %w", err)
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown http server: %w", err)
	}
	if err := <-serveErr; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("http server: %w", err)
	}

	logger.Info("shutdown complete", slog.String("role", string(cfg.Role)))
	return nil
}
