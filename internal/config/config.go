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
}

// LookupFunc returns the value of an environment variable, or "" if unset.
// Injected so tests never mutate process state.
type LookupFunc func(key string) string

// Load builds a Config from the given environment lookup, applying
// defaults for unset keys and rejecting malformed values.
func Load(lookup LookupFunc) (Config, error) {
	cfg := Config{
		Role:            RoleAll,
		HTTPPort:        defaultHTTPPort,
		ShutdownTimeout: defaultShutdownTimeout,
		LogLevel:        defaultLogLevel,
		DatabaseURL:     defaultDatabaseURL,
		RabbitURL:       defaultRabbitURL,
		WorkerPrefetch:  defaultWorkerPrefetch,
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
