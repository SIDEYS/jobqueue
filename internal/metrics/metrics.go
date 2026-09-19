// Package metrics defines the Prometheus instruments this system exposes
// and the functions that record them. It intentionally has no dependency
// on internal/store: recording a business event ("a job succeeded") is an
// orchestration-layer concern, decided by queue/worker/reaper/scheduler
// code that already knows what actually happened, not something bolted
// onto the raw persistence layer. The live-query collectors in
// collectors.go are the one exception - they need to read current state
// directly, and live in this package too, but as the only file here that
// imports internal/store.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var labels = []string{"queue", "job_type"}

var (
	jobsEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_enqueued_total",
		Help: "Jobs enqueued, by queue and job type. Idempotent duplicate enqueues are not counted - only genuine new jobs.",
	}, labels)

	jobsSucceeded = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_succeeded_total",
		Help: "Jobs that completed successfully, by queue and job type.",
	}, labels)

	jobsFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_failed_total",
		Help: "Job attempts that failed, by queue and job type - counted per attempt, whether or not that attempt was retried.",
	}, labels)

	jobsDead = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "jobs_dead_total",
		Help: "Jobs that were dead-lettered (max_attempts exhausted), by queue and job type.",
	}, labels)

	jobDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "job_duration_seconds",
		Help:    "Wall-clock time a job's handler took to run, by queue and job type. Measured around the handler call itself, not inferred from row timestamps.",
		Buckets: prometheus.DefBuckets,
	}, labels)

	// Named queue wait time, not "claim latency": what this measures is
	// claimed_at - run_at, i.e. how long a job sat ready-to-run before a
	// worker actually claimed it - the genuinely interesting number for
	// understanding worker capacity. "Claim latency" reads as "how long
	// the claim query itself took," which is a different (and much
	// smaller, uninteresting) thing.
	jobQueueWait = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "job_queue_wait_seconds",
		Help:    "Time between a job's run_at becoming due and the moment it was claimed, by queue and job type.",
		Buckets: prometheus.DefBuckets,
	}, labels)
)

func RecordEnqueued(queue, jobType string) {
	jobsEnqueued.WithLabelValues(queue, jobType).Inc()
}

func RecordSucceeded(queue, jobType string) {
	jobsSucceeded.WithLabelValues(queue, jobType).Inc()
}

func RecordFailed(queue, jobType string) {
	jobsFailed.WithLabelValues(queue, jobType).Inc()
}

func RecordDead(queue, jobType string) {
	jobsDead.WithLabelValues(queue, jobType).Inc()
}

func RecordJobDuration(queue, jobType string, d time.Duration) {
	jobDuration.WithLabelValues(queue, jobType).Observe(d.Seconds())
}

func RecordQueueWait(queue, jobType string, d time.Duration) {
	jobQueueWait.WithLabelValues(queue, jobType).Observe(d.Seconds())
}
