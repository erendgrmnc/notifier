package api

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"notifier/internal/domain"
	"notifier/internal/observability"
)

// MetricsGatherer exposes the local registry for summarizing.
type MetricsGatherer interface {
	GatherFamilies() ([]*dto.MetricFamily, error)
}

// LifetimeCounter reads durable totals from the database; they survive
// the process restarts that reset Prometheus counters.
type LifetimeCounter interface {
	CountNotificationStatuses(ctx context.Context) ([]domain.StatusCount, error)
}

// workerMetricsTimeout bounds the cross-service metrics fetch.
const workerMetricsTimeout = 2 * time.Second

// metricsSummary is the dashboard-friendly JSON view of the Prometheus
// counters. In a split api/worker deployment the delivery-side series
// live in the worker process; WorkerMetricsURL merges them in.
type metricsSummaryHandler struct {
	local     MetricsGatherer
	lifetime  LifetimeCounter
	workerURL string
	client    *http.Client
}

func newMetricsSummaryHandler(local MetricsGatherer, lifetime LifetimeCounter, workerURL string) *metricsSummaryHandler {
	return &metricsSummaryHandler{
		local:     local,
		lifetime:  lifetime,
		workerURL: workerURL,
		client:    &http.Client{Timeout: workerMetricsTimeout},
	}
}

type labeledValue struct {
	Labels map[string]string `json:"labels"`
	Value  float64           `json:"value"`
}

type latencySummary struct {
	Labels map[string]string `json:"labels"`
	Count  uint64            `json:"count"`
	AvgMS  float64           `json:"avg_ms"`
}

type lifetimeCount struct {
	Channel string `json:"channel"`
	Status  string `json:"status"`
	Count   int    `json:"count"`
}

type metricsSummary struct {
	Created         []labeledValue   `json:"created"`
	Delivered       []labeledValue   `json:"delivered"`
	Attempts        []labeledValue   `json:"attempts"`
	DeliveryLatency []latencySummary `json:"delivery_latency"`
	HTTPLatency     []latencySummary `json:"http_latency"`
	// Lifetime comes from the notifications table, not Prometheus, so it
	// survives process restarts.
	Lifetime []lifetimeCount `json:"lifetime"`
	Sources  []string        `json:"sources"`
}

func (handler *metricsSummaryHandler) serve(writer http.ResponseWriter, request *http.Request) {
	summary := metricsSummary{Sources: []string{"api"}}

	localFamilies, err := handler.local.GatherFamilies()
	if err != nil {
		writeErrorResponse(writer, http.StatusInternalServerError, "gather metrics: "+err.Error(), nil)
		return
	}
	accumulate(&summary, localFamilies)

	// Best effort: a split-role deployment merges the worker's series;
	// a missing worker just means api-only numbers.
	if handler.workerURL != "" {
		if workerFamilies, err := handler.fetchWorkerFamilies(request); err == nil {
			accumulate(&summary, workerFamilies)
			summary.Sources = append(summary.Sources, "worker")
		}
	}

	if handler.lifetime != nil {
		if counts, err := handler.lifetime.CountNotificationStatuses(request.Context()); err == nil {
			for _, count := range counts {
				summary.Lifetime = append(summary.Lifetime, lifetimeCount{
					Channel: string(count.Channel), Status: string(count.Status), Count: count.Count,
				})
			}
			summary.Sources = append(summary.Sources, "database")
		}
	}

	sortSummary(&summary)
	writeJSONResponse(writer, http.StatusOK, summary)
}

func (handler *metricsSummaryHandler) fetchWorkerFamilies(request *http.Request) (map[string]*dto.MetricFamily, error) {
	workerRequest, err := http.NewRequestWithContext(request.Context(), http.MethodGet, handler.workerURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build worker metrics request: %w", err)
	}
	response, err := handler.client.Do(workerRequest)
	if err != nil {
		return nil, fmt.Errorf("fetch worker metrics: %w", err)
	}
	defer response.Body.Close()

	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(response.Body)
	if err != nil {
		return nil, fmt.Errorf("parse worker metrics: %w", err)
	}
	return families, nil
}

// accumulate folds metric families into the summary. Accepts both the
// slice form (local registry) and map form (parsed text exposition).
func accumulate(summary *metricsSummary, families any) {
	visit := func(family *dto.MetricFamily) {
		switch family.GetName() {
		case "notifications_created_total":
			summary.Created = mergeCounters(summary.Created, family)
		case "notifications_delivered_total":
			summary.Delivered = mergeCounters(summary.Delivered, family)
		case "delivery_attempts_total":
			summary.Attempts = mergeCounters(summary.Attempts, family)
		case "delivery_duration_seconds":
			summary.DeliveryLatency = mergeHistograms(summary.DeliveryLatency, family)
		case "http_request_duration_seconds":
			summary.HTTPLatency = mergeHistograms(summary.HTTPLatency, family)
		}
	}
	switch typed := families.(type) {
	case []*dto.MetricFamily:
		for _, family := range typed {
			visit(family)
		}
	case map[string]*dto.MetricFamily:
		for _, family := range typed {
			visit(family)
		}
	}
}

func labelsOf(metric *dto.Metric) map[string]string {
	labels := make(map[string]string, len(metric.GetLabel()))
	for _, pair := range metric.GetLabel() {
		labels[pair.GetName()] = pair.GetValue()
	}
	return labels
}

func sameLabels(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}

func mergeCounters(existing []labeledValue, family *dto.MetricFamily) []labeledValue {
	for _, metric := range family.GetMetric() {
		labels := labelsOf(metric)
		value := metric.GetCounter().GetValue()
		merged := false
		for i := range existing {
			if sameLabels(existing[i].Labels, labels) {
				existing[i].Value += value
				merged = true
				break
			}
		}
		if !merged {
			existing = append(existing, labeledValue{Labels: labels, Value: value})
		}
	}
	return existing
}

func mergeHistograms(existing []latencySummary, family *dto.MetricFamily) []latencySummary {
	for _, metric := range family.GetMetric() {
		histogram := metric.GetHistogram()
		labels := labelsOf(metric)
		count := histogram.GetSampleCount()
		sum := histogram.GetSampleSum()
		merged := false
		for i := range existing {
			if sameLabels(existing[i].Labels, labels) {
				total := existing[i].AvgMS/1000*float64(existing[i].Count) + sum
				existing[i].Count += count
				if existing[i].Count > 0 {
					existing[i].AvgMS = total / float64(existing[i].Count) * 1000
				}
				merged = true
				break
			}
		}
		if !merged && count > 0 {
			existing = append(existing, latencySummary{
				Labels: labels, Count: count, AvgMS: sum / float64(count) * 1000,
			})
		}
	}
	return existing
}

func sortSummary(summary *metricsSummary) {
	byLabel := func(items []labeledValue) {
		sort.Slice(items, func(i, j int) bool {
			return fmt.Sprint(items[i].Labels) < fmt.Sprint(items[j].Labels)
		})
	}
	byLabel(summary.Created)
	byLabel(summary.Delivered)
	byLabel(summary.Attempts)
	// Busiest HTTP routes first; cap for the dashboard.
	sort.Slice(summary.HTTPLatency, func(i, j int) bool {
		return summary.HTTPLatency[i].Count > summary.HTTPLatency[j].Count
	})
	const maxHTTPRows = 8
	if len(summary.HTTPLatency) > maxHTTPRows {
		summary.HTTPLatency = summary.HTTPLatency[:maxHTTPRows]
	}
}

// Compile-time check that the concrete metrics type satisfies the
// gatherer interface used here.
var _ MetricsGatherer = (*observability.Metrics)(nil)
