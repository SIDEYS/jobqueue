package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// collectorQueryTimeout bounds every query a collector runs on scrape.
// Prometheus itself has its own scrape_timeout, but a bounded query here
// means one bad scrape can't hang a connection open indefinitely from this
// side either - defense in depth, not a substitute for the scraper's own
// timeout.
const collectorQueryTimeout = 5 * time.Second

// dbCollectors implements prometheus.Collector for metrics that only make
// sense as a live read of current state - queue depth and active worker
// count aren't events to increment a counter on, they're queried fresh on
// every scrape.
type dbCollectors struct {
	pool *pgxpool.Pool

	queueDepth    *prometheus.Desc
	activeWorkers *prometheus.Desc
}

// NewDBCollectors returns a prometheus.Collector that queries pool
// directly on every scrape. Register it once with prometheus.MustRegister.
func NewDBCollectors(pool *pgxpool.Pool) prometheus.Collector {
	return &dbCollectors{
		pool: pool,
		queueDepth: prometheus.NewDesc(
			"queue_depth",
			"Number of pending or running jobs, by queue and status.",
			[]string{"queue", "status"}, nil,
		),
		activeWorkers: prometheus.NewDesc(
			"active_workers",
			"Number of worker rows currently within their heartbeat TTL (stale rows are deleted by the heartbeat loop itself, so this is simply every remaining row).",
			nil, nil,
		),
	}
}

func (c *dbCollectors) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.queueDepth
	ch <- c.activeWorkers
}

func (c *dbCollectors) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), collectorQueryTimeout)
	defer cancel()

	c.collectQueueDepth(ctx, ch)
	c.collectActiveWorkers(ctx, ch)
}

// collectQueueDepth is restricted to pending/running, not all five
// statuses a job can ever be in - so this rides idx_jobs_queue_status
// (migration 0001), which was sized for exactly this query. Counting
// every status a job has ever passed through would mean a sequential scan
// over the whole table, run again every scrape interval, growing slower
// as the system's history grows.
func (c *dbCollectors) collectQueueDepth(ctx context.Context, ch chan<- prometheus.Metric) {
	rows, err := c.pool.Query(ctx, `
		SELECT queue, status, count(*) FROM jobs
		WHERE status IN ('pending', 'running')
		GROUP BY queue, status`)
	if err != nil {
		slog.Error("metrics: collect queue depth", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var queue, status string
		var count int64
		if err := rows.Scan(&queue, &status, &count); err != nil {
			slog.Error("metrics: scan queue depth row", "error", err)
			return
		}
		ch <- prometheus.MustNewConstMetric(c.queueDepth, prometheus.GaugeValue, float64(count), queue, status)
	}
	if err := rows.Err(); err != nil {
		slog.Error("metrics: collect queue depth", "error", err)
	}
}

func (c *dbCollectors) collectActiveWorkers(ctx context.Context, ch chan<- prometheus.Metric) {
	var count int64
	if err := c.pool.QueryRow(ctx, `SELECT count(*) FROM workers`).Scan(&count); err != nil {
		slog.Error("metrics: collect active workers", "error", err)
		return
	}
	ch <- prometheus.MustNewConstMetric(c.activeWorkers, prometheus.GaugeValue, float64(count))
}
