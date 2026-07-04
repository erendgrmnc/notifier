package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func newOpsRouter(readiness ReadinessChecks) http.Handler {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	return NewRouter(RouterConfig{Logger: logger, RequestTimeout: time.Second, Readiness: readiness})
}

func TestReadyzAllDependenciesUp(t *testing.T) {
	router := newOpsRouter(ReadinessChecks{
		"postgres": func(context.Context) error { return nil },
		"rabbitmq": func(context.Context) error { return nil },
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200", recorder.Code)
	}
	var response struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal readyz: %v", err)
	}
	if response.Status != "ok" || response.Checks["postgres"] != "ok" {
		t.Errorf("readyz body = %+v, want all ok", response)
	}
}

func TestReadyzDependencyDownReturns503(t *testing.T) {
	router := newOpsRouter(ReadinessChecks{
		"postgres": func(context.Context) error { return nil },
		"rabbitmq": func(context.Context) error { return errors.New("connection closed") },
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", recorder.Code)
	}
	var response struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal readyz: %v", err)
	}
	if response.Status != "degraded" || response.Checks["postgres"] != "ok" ||
		response.Checks["rabbitmq"] == "ok" {
		t.Errorf("readyz body = %+v, want degraded with rabbitmq down", response)
	}
}

func TestOpsRouterServesNoNotificationAPI(t *testing.T) {
	router := newOpsRouter(nil)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/notifications", nil))
	if recorder.Code != http.StatusNotFound {
		t.Errorf("ops router serves notification API: %d, want 404", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusOK {
		t.Errorf("ops router healthz = %d, want 200", recorder.Code)
	}
}
