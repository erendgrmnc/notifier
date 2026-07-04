package observability

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsInstrumentsRecord(t *testing.T) {
	metrics := NewMetrics()

	metrics.NotificationCreated("sms", "high")
	metrics.NotificationCreated("sms", "high")
	metrics.NotificationDelivered("sms", "sent")
	metrics.DeliveryAttempt("sms", "success", 42*time.Millisecond)
	metrics.DeliveryAttempt("sms", "retryable", time.Second)
	metrics.ObserveHTTPRequest("/api/v1/notifications", "POST", 201, 5*time.Millisecond)
	metrics.SetQueueDepth("notifications.sms", 7)

	if got := testutil.ToFloat64(metrics.created.WithLabelValues("sms", "high")); got != 2 {
		t.Errorf("created counter = %v, want 2", got)
	}
	if got := testutil.ToFloat64(metrics.delivered.WithLabelValues("sms", "sent")); got != 1 {
		t.Errorf("delivered counter = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.attempts.WithLabelValues("sms", "retryable")); got != 1 {
		t.Errorf("retryable attempts = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.queueDepth.WithLabelValues("notifications.sms")); got != 7 {
		t.Errorf("queue depth gauge = %v, want 7", got)
	}
}

func TestMetricsHandlerExposesSamples(t *testing.T) {
	metrics := NewMetrics()
	metrics.NotificationCreated("email", "normal")
	metrics.ObserveHTTPRequest("/healthz", "GET", 200, time.Millisecond)

	recorder := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))

	if recorder.Code != 200 {
		t.Fatalf("/metrics = %d, want 200", recorder.Code)
	}
	exposition := recorder.Body.String()
	for _, want := range []string{"notifications_created_total", `channel="email"`, "http_request_duration_seconds", "go_goroutines"} {
		if !strings.Contains(exposition, want) {
			t.Errorf("exposition missing %q", want)
		}
	}
}
