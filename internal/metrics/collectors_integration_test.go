//go:build integration

package metrics_test

import (
	"context"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/SIDEYS/jobqueue/internal/metrics"
	"github.com/SIDEYS/jobqueue/internal/store"
)

func startPostgres(t *testing.T, ctx context.Context) string {
	t.Helper()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("jobqueue"),
		postgres.WithUsername("jobqueue"),
		postgres.WithPassword("jobqueue"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ctr.Terminate(context.Background()))
	})

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return connStr
}

// gaugeValue finds a single gauge sample matching name and labels among
// gathered metric families, so the test can assert on specific label
// combinations without hand-writing the full expfmt text output.
func gaugeValue(mfs []*dto.MetricFamily, name string, labels map[string]string) (float64, bool) {
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			match := true
			for k, v := range labels {
				found := false
				for _, lp := range m.GetLabel() {
					if lp.GetName() == k && lp.GetValue() == v {
						found = true
						break
					}
				}
				if !found {
					match = false
					break
				}
			}
			if match {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// TestDBCollectorsQueueDepthAndActiveWorkers seeds jobs across every
// status and two queues, plus two heartbeated workers, and asserts the
// collector reports pending/running counts per queue correctly, excludes
// succeeded jobs from queue_depth entirely (it's restricted to
// pending/running on purpose - see collectors.go), and counts active
// workers correctly.
func TestDBCollectorsQueueDepthAndActiveWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(s.Close)

	_, err = s.Pool().Exec(ctx, `
		INSERT INTO jobs (queue, job_type, payload, status, run_at)
		SELECT 'collector-test', 'sleep', '{}'::jsonb, 'pending', now() FROM generate_series(1, 3)`)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `
		INSERT INTO jobs (queue, job_type, payload, status, run_at, claimed_by, claimed_at)
		VALUES ('collector-test', 'sleep', '{}'::jsonb, 'running', now(), 'w1', now())`)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `
		INSERT INTO jobs (queue, job_type, payload, status, run_at)
		VALUES ('collector-test', 'sleep', '{}'::jsonb, 'succeeded', now())`)
	require.NoError(t, err)
	_, err = s.Pool().Exec(ctx, `
		INSERT INTO jobs (queue, job_type, payload, status, run_at)
		SELECT 'other-queue', 'sleep', '{}'::jsonb, 'pending', now() FROM generate_series(1, 2)`)
	require.NoError(t, err)

	require.NoError(t, s.UpsertHeartbeat(ctx, "worker-1", "host-a", 1))
	require.NoError(t, s.UpsertHeartbeat(ctx, "worker-2", "host-b", 0))

	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.NewDBCollectors(s.Pool()))

	mfs, err := registry.Gather()
	require.NoError(t, err)

	pending, ok := gaugeValue(mfs, "queue_depth", map[string]string{"queue": "collector-test", "status": "pending"})
	require.True(t, ok, "expected a queue_depth series for collector-test/pending")
	require.Equal(t, float64(3), pending)

	running, ok := gaugeValue(mfs, "queue_depth", map[string]string{"queue": "collector-test", "status": "running"})
	require.True(t, ok, "expected a queue_depth series for collector-test/running")
	require.Equal(t, float64(1), running)

	_, ok = gaugeValue(mfs, "queue_depth", map[string]string{"queue": "collector-test", "status": "succeeded"})
	require.False(t, ok, "queue_depth must not report succeeded jobs")

	other, ok := gaugeValue(mfs, "queue_depth", map[string]string{"queue": "other-queue", "status": "pending"})
	require.True(t, ok, "expected a queue_depth series for other-queue/pending")
	require.Equal(t, float64(2), other)

	activeWorkers, ok := gaugeValue(mfs, "active_workers", nil)
	require.True(t, ok, "expected an active_workers series")
	require.Equal(t, float64(2), activeWorkers)
}
