package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"notifier/internal/observability"
)

const workerExposition = `# HELP notifications_delivered_total x
# TYPE notifications_delivered_total counter
notifications_delivered_total{channel="sms",status="sent"} 12
notifications_delivered_total{channel="sms",status="failed"} 3
# HELP delivery_attempts_total x
# TYPE delivery_attempts_total counter
delivery_attempts_total{channel="sms",outcome="success"} 12
delivery_attempts_total{channel="sms",outcome="retryable"} 5
# HELP delivery_duration_seconds x
# TYPE delivery_duration_seconds histogram
delivery_duration_seconds_bucket{channel="sms",le="+Inf"} 17
delivery_duration_seconds_sum{channel="sms"} 1.7
delivery_duration_seconds_count{channel="sms"} 17
`

func TestMetricsSummaryMergesWorkerSeries(t *testing.T) {
	workerServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(writer, workerExposition)
	}))
	defer workerServer.Close()

	local := observability.NewMetrics()
	local.NotificationCreated("sms", "high")
	local.ObserveHTTPRequest("/api/v1/notifications", "POST", 201, 3*time.Millisecond)

	handler := newMetricsSummaryHandler(local, workerServer.URL)
	recorder := httptest.NewRecorder()
	handler.serve(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/summary", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("summary = %d, want 200", recorder.Code)
	}
	var summary metricsSummary
	if err := json.Unmarshal(recorder.Body.Bytes(), &summary); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}

	if len(summary.Sources) != 2 {
		t.Errorf("sources = %v, want api+worker", summary.Sources)
	}
	if len(summary.Created) != 1 || summary.Created[0].Value != 1 {
		t.Errorf("created = %+v, want one sms/high sample", summary.Created)
	}
	var sent float64
	for _, delivered := range summary.Delivered {
		if delivered.Labels["status"] == "sent" {
			sent = delivered.Value
		}
	}
	if sent != 12 {
		t.Errorf("delivered sent = %v, want 12 from worker", sent)
	}
	if len(summary.DeliveryLatency) != 1 || summary.DeliveryLatency[0].Count != 17 {
		t.Errorf("delivery latency = %+v, want count 17", summary.DeliveryLatency)
	}
	wantAvg := 1.7 / 17 * 1000
	if diff := summary.DeliveryLatency[0].AvgMS - wantAvg; diff > 0.01 || diff < -0.01 {
		t.Errorf("delivery avg = %v ms, want %v", summary.DeliveryLatency[0].AvgMS, wantAvg)
	}
}

func TestMetricsSummaryWorksWithoutWorker(t *testing.T) {
	local := observability.NewMetrics()
	local.NotificationCreated("email", "normal")

	handler := newMetricsSummaryHandler(local, "")
	recorder := httptest.NewRecorder()
	handler.serve(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/summary", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("summary = %d, want 200", recorder.Code)
	}
	var summary metricsSummary
	if err := json.Unmarshal(recorder.Body.Bytes(), &summary); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}
	if len(summary.Sources) != 1 || summary.Sources[0] != "api" {
		t.Errorf("sources = %v, want api only", summary.Sources)
	}
}
