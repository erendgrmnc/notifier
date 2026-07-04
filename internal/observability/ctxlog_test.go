package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestCorrelationIDRoundTrip(t *testing.T) {
	ctx := WithCorrelationID(context.Background(), "abc-123")

	if got := CorrelationIDFrom(ctx); got != "abc-123" {
		t.Errorf("CorrelationIDFrom = %q, want %q", got, "abc-123")
	}
}

func TestCorrelationIDMissing(t *testing.T) {
	if got := CorrelationIDFrom(context.Background()); got != "" {
		t.Errorf("CorrelationIDFrom on empty ctx = %q, want empty", got)
	}
}

func TestLoggerFromIncludesCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))

	ctx := WithCorrelationID(context.Background(), "abc-123")
	LoggerFrom(ctx, base).Info("hello")

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}
	if line["correlation_id"] != "abc-123" {
		t.Errorf("correlation_id = %v, want abc-123", line["correlation_id"])
	}
}

func TestLoggerFromWithoutCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	base := slog.New(slog.NewJSONHandler(&buf, nil))

	LoggerFrom(context.Background(), base).Info("hello")

	var line map[string]any
	if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
		t.Fatalf("unmarshal log line: %v", err)
	}
	if _, present := line["correlation_id"]; present {
		t.Error("correlation_id present on log line, want absent")
	}
}

func TestNewLoggerHonorsLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(&buf, "warn")

	logger.Info("suppressed")
	if buf.Len() != 0 {
		t.Errorf("info line emitted at warn level: %s", buf.String())
	}

	logger.Warn("emitted")
	if buf.Len() == 0 {
		t.Error("warn line not emitted at warn level")
	}
}
