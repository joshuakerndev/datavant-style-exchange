package metrics

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	outboxPendingTimeout = 2 * time.Second
)

var (
	IngestRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ingest_requests_total",
			Help: "Total ingest requests by route and status.",
		},
		[]string{"route", "status"},
	)
	OutboxPending = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "outbox_pending",
			Help: "Pending outbox events not yet published.",
		},
	)
	OutboxPublishFailures = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "outbox_publish_failures_total",
			Help: "Total outbox publish failures.",
		},
	)
	MetricsScrapeErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "metrics_scrape_errors_total",
			Help: "Total metrics scrape errors.",
		},
	)
)

func init() {
	prometheus.MustRegister(IngestRequests, OutboxPending, OutboxPublishFailures, MetricsScrapeErrors)
}

func Handler(db *sql.DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		updateOutboxPending(db)
		promhttp.Handler().ServeHTTP(w, r)
	})
}

func updateOutboxPending(db *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), outboxPendingTimeout)
	defer cancel()

	var pending int64
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM outbox_events WHERE published_at IS NULL").Scan(&pending); err != nil {
		MetricsScrapeErrors.Inc()
		return
	}

	OutboxPending.Set(float64(pending))
}
