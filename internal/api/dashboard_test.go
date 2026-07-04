package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"notifier/internal/mockprovider"
	"notifier/internal/queue/rabbit"
)

type fakeWorkerControl struct {
	paused bool
}

func (fake *fakeWorkerControl) WorkerPaused(context.Context) (bool, error) {
	return fake.paused, nil
}

func (fake *fakeWorkerControl) SetWorkerPaused(_ context.Context, paused bool) error {
	fake.paused = paused
	return nil
}

type fakeQueueInspector struct{}

func (fakeQueueInspector) QueueDepths() ([]rabbit.QueueDepth, error) {
	return []rabbit.QueueDepth{{Name: "notifications.sms", Ready: 3}}, nil
}

func newDashboardRouter(enabled bool) (http.Handler, *fakeWorkerControl) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	control := &fakeWorkerControl{}
	router := NewRouter(RouterConfig{
		Logger:           logger,
		RequestTimeout:   time.Second,
		Notifications:    newFakeNotificationService(),
		DashboardEnabled: enabled,
		WorkerControl:    control,
		Queues:           fakeQueueInspector{},
		ProviderStore:    mockprovider.NewStore(),
	})
	return router, control
}

func TestDashboardRoutesGatedByFlag(t *testing.T) {
	router, _ := newDashboardRouter(false)

	for _, path := range []string{"/dashboard", "/api/v1/queues", "/api/v1/worker", "/api/v1/provider/messages"} {
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusNotFound {
			t.Errorf("GET %s with dashboard disabled = %d, want 404", path, recorder.Code)
		}
	}
}

func TestDashboardPageServed(t *testing.T) {
	router, _ := newDashboardRouter(true)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dashboard", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /dashboard = %d, want 200", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), "Testing Dashboard") {
		t.Error("dashboard page content missing")
	}
}

func TestWorkerToggleRoundTrip(t *testing.T) {
	router, control := newDashboardRouter(true)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPut, "/api/v1/worker", strings.NewReader(`{"paused":true}`)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("PUT /api/v1/worker = %d, want 200", recorder.Code)
	}
	if !control.paused {
		t.Error("pause flag not set")
	}

	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/worker", nil))
	var state map[string]bool
	if err := json.Unmarshal(recorder.Body.Bytes(), &state); err != nil {
		t.Fatalf("unmarshal worker state: %v", err)
	}
	if !state["paused"] {
		t.Error("GET /api/v1/worker did not reflect paused state")
	}
}

func TestQueueDepthsServed(t *testing.T) {
	router, _ := newDashboardRouter(true)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/queues", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/queues = %d, want 200", recorder.Code)
	}
	if !strings.Contains(recorder.Body.String(), `"notifications.sms"`) {
		t.Errorf("queue depths body missing queue name: %s", recorder.Body.String())
	}
}

func TestProviderReceiveAndListRoundTrip(t *testing.T) {
	router, _ := newDashboardRouter(true)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/provider/messages",
		strings.NewReader(`{"to":"+905551234567","channel":"sms","content":"delivered!"}`)))
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("POST /provider/messages = %d, want 202", recorder.Code)
	}

	recorder = httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/provider/messages", nil))
	if !strings.Contains(recorder.Body.String(), "delivered!") {
		t.Errorf("provider messages missing delivery: %s", recorder.Body.String())
	}
}

func TestListNotificationsRejectsBadLimit(t *testing.T) {
	router := newRouterWithService(newFakeNotificationService())

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/notifications?limit=zero", nil))
	if recorder.Code != http.StatusBadRequest {
		t.Errorf("bad limit = %d, want 400", recorder.Code)
	}
}
