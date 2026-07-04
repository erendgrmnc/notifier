package config

import (
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(func(string) string { return "" })
	if err != nil {
		t.Fatalf("Load with defaults: %v", err)
	}

	if cfg.Role != RoleAll {
		t.Errorf("Role = %q, want %q", cfg.Role, RoleAll)
	}
	if cfg.HTTPPort != defaultHTTPPort {
		t.Errorf("HTTPPort = %d, want %d", cfg.HTTPPort, defaultHTTPPort)
	}
	if cfg.ShutdownTimeout != defaultShutdownTimeout {
		t.Errorf("ShutdownTimeout = %v, want %v", cfg.ShutdownTimeout, defaultShutdownTimeout)
	}
	if cfg.LogLevel != defaultLogLevel {
		t.Errorf("LogLevel = %q, want %q", cfg.LogLevel, defaultLogLevel)
	}
}

func TestLoadFromEnvironment(t *testing.T) {
	environment := map[string]string{
		"ROLE":             "worker",
		"HTTP_PORT":        "9000",
		"SHUTDOWN_TIMEOUT": "5s",
		"LOG_LEVEL":        "debug",
	}
	cfg, err := Load(func(key string) string { return environment[key] })
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Role != RoleWorker {
		t.Errorf("Role = %q, want %q", cfg.Role, RoleWorker)
	}
	if cfg.HTTPPort != 9000 {
		t.Errorf("HTTPPort = %d, want 9000", cfg.HTTPPort)
	}
	if cfg.ShutdownTimeout != 5*time.Second {
		t.Errorf("ShutdownTimeout = %v, want 5s", cfg.ShutdownTimeout)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug", cfg.LogLevel)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	testCases := []struct {
		name string
		key  string
		val  string
	}{
		{name: "unknown role", key: "ROLE", val: "gateway"},
		{name: "non-numeric port", key: "HTTP_PORT", val: "eighty"},
		{name: "malformed duration", key: "SHUTDOWN_TIMEOUT", val: "10 minutes"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			lookup := func(key string) string {
				if key == tc.key {
					return tc.val
				}
				return ""
			}
			if _, err := Load(lookup); err == nil {
				t.Fatalf("Load accepted %s=%q, want error", tc.key, tc.val)
			}
		})
	}
}
