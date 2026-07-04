package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns the service's Prometheus registry and instruments. It is
// injected into layers through consumer-side interfaces; this concrete
// type satisfies all of them.
type Metrics struct {
	registry *prometheus.Registry

	created          *prometheus.CounterVec
	delivered        *prometheus.CounterVec
	attempts         *prometheus.CounterVec
	deliveryDuration *prometheus.HistogramVec
	httpDuration     *prometheus.HistogramVec
	queueDepth       *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(collectors.NewGoCollector())
	factory := promauto.With(registry)

	return &Metrics{
		registry: registry,
		created: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_created_total",
			Help: "Notifications accepted by the API, by channel and priority.",
		}, []string{"channel", "priority"}),
		delivered: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "notifications_delivered_total",
			Help: "Notifications reaching a terminal delivery state, by channel and status (sent|failed).",
		}, []string{"channel", "status"}),
		attempts: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "delivery_attempts_total",
			Help: "Provider delivery attempts, by channel and outcome (success|retryable|permanent).",
		}, []string{"channel", "outcome"}),
		deliveryDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "delivery_duration_seconds",
			Help:    "Provider send latency per attempt.",
			Buckets: prometheus.DefBuckets,
		}, []string{"channel"}),
		httpDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency by chi route pattern.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method", "code"}),
		queueDepth: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "queue_depth",
			Help: "Ready messages per queue, sampled every few seconds.",
		}, []string{"queue"}),
	}
}

// Handler serves the registry for Prometheus scrapes.
func (metrics *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(metrics.registry, promhttp.HandlerOpts{})
}

func (metrics *Metrics) NotificationCreated(channel, priority string) {
	metrics.created.WithLabelValues(channel, priority).Inc()
}

func (metrics *Metrics) NotificationDelivered(channel, status string) {
	metrics.delivered.WithLabelValues(channel, status).Inc()
}

func (metrics *Metrics) DeliveryAttempt(channel, outcome string, duration time.Duration) {
	metrics.attempts.WithLabelValues(channel, outcome).Inc()
	metrics.deliveryDuration.WithLabelValues(channel).Observe(duration.Seconds())
}

func (metrics *Metrics) ObserveHTTPRequest(route, method string, statusCode int, duration time.Duration) {
	metrics.httpDuration.WithLabelValues(route, method, strconv.Itoa(statusCode)).Observe(duration.Seconds())
}

func (metrics *Metrics) SetQueueDepth(queue string, ready int) {
	metrics.queueDepth.WithLabelValues(queue).Set(float64(ready))
}
