// Package config loads all environment-dependent settings for the service.
package config

import (
	"fmt"
	"strconv"
	"time"
)

// Role selects which components a process runs.
type Role string

const (
	RoleAPI    Role = "api"
	RoleWorker Role = "worker"
	RoleAll    Role = "all"
)

const (
	defaultHTTPPort        = 8081
	defaultShutdownTimeout = 15 * time.Second
	defaultLogLevel        = "info"
	defaultDatabaseURL     = "postgres://notifier:notifier@localhost:5432/notifier?sslmode=disable"
	defaultRabbitURL       = "amqp://notifier:notifier@localhost:5672/"
	defaultWorkerPrefetch  = 50
	defaultProviderTimeout = 10 * time.Second
	// defaultMaxDeliveryAttempts = first attempt + one per retry tier.
	defaultMaxDeliveryAttempts = 4
	// defaultMaxBatchSize is the assessment's batch ceiling.
	defaultMaxBatchSize = 1000
	// defaultRateLimitPerChannel is the assessment's 100 msg/s ceiling.
	defaultRateLimitPerChannel = 100
	// defaultWorkerConcurrency is handler goroutines per channel queue.
	defaultWorkerConcurrency = 10
	// defaultSchedulerPollInterval is how often due/stuck rows are claimed.
	defaultSchedulerPollInterval = time.Second
	// defaultStalePendingAfter is how long a pending/queued row may sit
	// before the sweeper assumes its publish was lost.
	defaultStalePendingAfter = time.Minute
)

// Config holds every runtime tunable. Values come from environment
// variables; nothing here may be hardcoded elsewhere in the codebase.
type Config struct {
	Role            Role
	HTTPPort        int
	ShutdownTimeout time.Duration
	LogLevel        string
	DatabaseURL     string
	RabbitURL       string
	WorkerPrefetch  int
	// ProviderURL is the external delivery endpoint (a webhook.site URL).
	// Empty means deliveries are simulated with the logging sender.
	ProviderURL         string
	ProviderTimeout     time.Duration
	MaxDeliveryAttempts int
	// MaxBatchSize caps items per batch create request.
	MaxBatchSize int
	// DashboardEnabled mounts the testing dashboard, worker controls,
	// queue inspection, and the built-in mock provider.
	DashboardEnabled bool
	// WorkerMetricsURL lets a split-role api merge the worker's metrics
	// into the dashboard summary. Empty skips the merge.
	WorkerMetricsURL string
	// RateLimitPerChannel caps deliveries per second per channel. With N
	// worker replicas the effective limit is N× this value; set to
	// limit/N when scaling out.
	RateLimitPerChannel int
	// WorkerConcurrency is concurrent delivery handlers per channel.
	WorkerConcurrency int
	// SchedulerPollInterval paces the due-notification claimer.
	SchedulerPollInterval time.Duration
	// StalePendingAfter is the sweep cutoff for lost publishes.
	StalePendingAfter time.Duration
}

// LookupFunc returns the value of an environment variable, or "" if unset.
// Injected so tests never mutate process state.
type LookupFunc func(key string) string

// Load builds a Config from the given environment lookup, applying
// defaults for unset keys and rejecting malformed values.
func Load(lookup LookupFunc) (Config, error) {
	cfg := Config{
		Role:                  RoleAll,
		HTTPPort:              defaultHTTPPort,
		ShutdownTimeout:       defaultShutdownTimeout,
		LogLevel:              defaultLogLevel,
		DatabaseURL:           defaultDatabaseURL,
		RabbitURL:             defaultRabbitURL,
		WorkerPrefetch:        defaultWorkerPrefetch,
		ProviderTimeout:       defaultProviderTimeout,
		MaxDeliveryAttempts:   defaultMaxDeliveryAttempts,
		MaxBatchSize:          defaultMaxBatchSize,
		RateLimitPerChannel:   defaultRateLimitPerChannel,
		WorkerConcurrency:     defaultWorkerConcurrency,
		SchedulerPollInterval: defaultSchedulerPollInterval,
		StalePendingAfter:     defaultStalePendingAfter,
	}

	if roleValue := lookup("ROLE"); roleValue != "" {
		role := Role(roleValue)
		switch role {
		case RoleAPI, RoleWorker, RoleAll:
			cfg.Role = role
		default:
			return Config{}, fmt.Errorf("parse ROLE: unknown role %q", roleValue)
		}
	}

	if err := parseInt(lookup, "HTTP_PORT", &cfg.HTTPPort); err != nil {
		return Config{}, err
	}
	if err := parseInt(lookup, "WORKER_PREFETCH", &cfg.WorkerPrefetch); err != nil {
		return Config{}, err
	}
	if err := parseDuration(lookup, "SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout); err != nil {
		return Config{}, err
	}
	if levelValue := lookup("LOG_LEVEL"); levelValue != "" {
		cfg.LogLevel = levelValue
	}
	if databaseURL := lookup("DATABASE_URL"); databaseURL != "" {
		cfg.DatabaseURL = databaseURL
	}
	if rabbitURL := lookup("RABBITMQ_URL"); rabbitURL != "" {
		cfg.RabbitURL = rabbitURL
	}
	cfg.ProviderURL = lookup("PROVIDER_URL")
	if err := parseDuration(lookup, "PROVIDER_TIMEOUT", &cfg.ProviderTimeout); err != nil {
		return Config{}, err
	}
	if err := parseInt(lookup, "MAX_BATCH_SIZE", &cfg.MaxBatchSize); err != nil {
		return Config{}, err
	}
	if err := parseInt(lookup, "MAX_DELIVERY_ATTEMPTS", &cfg.MaxDeliveryAttempts); err != nil {
		return Config{}, err
	}
	if err := parseBool(lookup, "DASHBOARD_ENABLED", &cfg.DashboardEnabled); err != nil {
		return Config{}, err
	}
	cfg.WorkerMetricsURL = lookup("WORKER_METRICS_URL")
	if err := parseInt(lookup, "RATE_LIMIT_PER_CHANNEL", &cfg.RateLimitPerChannel); err != nil {
		return Config{}, err
	}
	if err := parseInt(lookup, "WORKER_CONCURRENCY", &cfg.WorkerConcurrency); err != nil {
		return Config{}, err
	}
	if err := parseDuration(lookup, "SCHEDULER_POLL_INTERVAL", &cfg.SchedulerPollInterval); err != nil {
		return Config{}, err
	}
	if err := parseDuration(lookup, "STALE_PENDING_AFTER", &cfg.StalePendingAfter); err != nil {
		return Config{}, err
	}

	// Zero or negative here would make the limiter reject every wait
	// (hot redelivery loop) or start zero handlers.
	if cfg.RateLimitPerChannel < 1 {
		return Config{}, fmt.Errorf("parse RATE_LIMIT_PER_CHANNEL: must be at least 1, got %d", cfg.RateLimitPerChannel)
	}
	if cfg.WorkerConcurrency < 1 {
		return Config{}, fmt.Errorf("parse WORKER_CONCURRENCY: must be at least 1, got %d", cfg.WorkerConcurrency)
	}
	if cfg.MaxBatchSize < 1 {
		return Config{}, fmt.Errorf("parse MAX_BATCH_SIZE: must be at least 1, got %d", cfg.MaxBatchSize)
	}

	return cfg, nil
}

func parseInt(lookup LookupFunc, key string, target *int) error {
	raw := lookup(key)
	if raw == "" {
		return nil
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil {
		return fmt.Errorf("parse %s: %w", key, err)
	}
	*target = parsed
	return nil
}

func parseBool(lookup LookupFunc, key string, target *bool) error {
	raw := lookup(key)
	if raw == "" {
		return nil
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return fmt.Errorf("parse %s: %w", key, err)
	}
	*target = parsed
	return nil
}

func parseDuration(lookup LookupFunc, key string, target *time.Duration) error {
	raw := lookup(key)
	if raw == "" {
		return nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fmt.Errorf("parse %s: %w", key, err)
	}
	*target = parsed
	return nil
}
