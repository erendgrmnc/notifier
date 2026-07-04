package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"notifier/internal/domain"
	"notifier/internal/observability"
)

type fakeLifetimeCounter struct{}

func (fakeLifetimeCounter) CountNotificationStatuses(context.Context) ([]domain.StatusCount, error) {
	return []domain.StatusCount{
		{Channel: domain.ChannelSMS, Status: domain.StatusSent, Count: 40},
		{Channel: domain.ChannelSMS, Status: domain.StatusFailed, Count: 2},
	}, nil
}

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

	handler := newMetricsSummaryHandler(local, fakeLifetimeCounter{}, workerServer.URL)
	recorder := httptest.NewRecorder()
	handler.serve(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/summary", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("summary = %d, want 200", recorder.Code)
	}
	var summary metricsSummary
	if err := json.Unmarshal(recorder.Body.Bytes(), &summary); err != nil {
		t.Fatalf("unmarshal summary: %v", err)
	}

	if len(summary.Sources) != 3 {
		t.Errorf("sources = %v, want api+worker+database", summary.Sources)
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
	if len(summary.Lifetime) != 2 || summary.Lifetime[0].Count+summary.Lifetime[1].Count != 42 {
		t.Errorf("lifetime = %+v, want DB totals summing 42", summary.Lifetime)
	}
	if len(summary.DeliveryLatency) != 1 || summary.DeliveryLatency[0].Count != 17 {
		t.Errorf("delivery latency = %+v, want count 17", summary.DeliveryLatency)
	}
	wantAvg := 1.7 / 17 * 1000
	if diff := summary.DeliveryLatency[0].AvgMS - wantAvg; diff > 0.01 || diff < -0.01 {
		t.Errorf("delivery avg = %v ms, want %v", summary.DeliveryLatency[0].AvgMS, wantAvg)
	}
}

// A failing worker fetch must not stall every summary request: the
// dashboard polls this endpoint every second and renders nothing until
// it answers, so repeated timeouts freeze the whole dashboard.
func TestMetricsSummarySkipsWorkerFetchDuringCooldown(t *testing.T) {
	fetches := 0
	healthy := false
	workerServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		fetches++
		if healthy {
			fmt.Fprint(writer, workerExposition)
			return
		}
		fmt.Fprint(writer, "not a metrics exposition {{{")
	}))
	defer workerServer.Close()

	handler := newMetricsSummaryHandler(observability.NewMetrics(), nil, workerServer.URL)
	current := time.Now()
	handler.now = func() time.Time { return current }

	summaryFor := func(t *testing.T) metricsSummary {
		t.Helper()
		recorder := httptest.NewRecorder()
		handler.serve(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/metrics/summary", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("summary = %d, want 200", recorder.Code)
		}
		var summary metricsSummary
		if err := json.Unmarshal(recorder.Body.Bytes(), &summary); err != nil {
			t.Fatalf("unmarshal summary: %v", err)
		}
		return summary
	}

	summaryFor(t) // first request discovers the failure
	if fetches != 1 {
		t.Fatalf("fetches after first serve = %d, want 1", fetches)
	}

	summaryFor(t) // within cooldown: no fetch attempt
	if fetches != 1 {
		t.Errorf("fetches within cooldown = %d, want still 1", fetches)
	}

	healthy = true
	current = current.Add(workerFetchCooldown + time.Second)
	summary := summaryFor(t) // cooldown elapsed: retry and recover
	if fetches != 2 {
		t.Errorf("fetches after cooldown = %d, want 2", fetches)
	}
	recovered := false
	for _, source := range summary.Sources {
		if source == "worker" {
			recovered = true
		}
	}
	if !recovered {
		t.Errorf("sources = %v, want worker merged back after recovery", summary.Sources)
	}
}

func TestMetricsSummaryWorksWithoutWorker(t *testing.T) {
	local := observability.NewMetrics()
	local.NotificationCreated("email", "normal")

	handler := newMetricsSummaryHandler(local, nil, "")
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
