package metrics

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// histogramSampleCount reads back how many observations a specific
// (queue, job_type) series has recorded, so a test can verify an
// observation actually landed - not just that the series exists (which
// would already be true after the very first observation, and wouldn't
// distinguish "observed once" from "observed twice").
func histogramSampleCount(t *testing.T, vec *prometheus.HistogramVec, queue, jobType string) uint64 {
	t.Helper()
	metric, ok := vec.WithLabelValues(queue, jobType).(prometheus.Metric)
	require.True(t, ok, "HistogramVec's Observer must also implement prometheus.Metric")

	var m dto.Metric
	require.NoError(t, metric.Write(&m))
	return m.GetHistogram().GetSampleCount()
}

func TestRecordEnqueuedIncrementsOnlyItsOwnLabels(t *testing.T) {
	before := testutil.ToFloat64(jobsEnqueued.WithLabelValues("metrics-test-q1", "t1"))

	RecordEnqueued("metrics-test-q1", "t1")

	after := testutil.ToFloat64(jobsEnqueued.WithLabelValues("metrics-test-q1", "t1"))
	require.Equal(t, before+1, after)

	// A different label pair must be unaffected.
	other := testutil.ToFloat64(jobsEnqueued.WithLabelValues("metrics-test-q2", "t1"))
	RecordEnqueued("metrics-test-q1", "t1")
	require.Equal(t, other, testutil.ToFloat64(jobsEnqueued.WithLabelValues("metrics-test-q2", "t1")))
}

func TestRecordSucceededFailedDead(t *testing.T) {
	queue, jobType := "metrics-test-outcomes", "t1"

	beforeS := testutil.ToFloat64(jobsSucceeded.WithLabelValues(queue, jobType))
	RecordSucceeded(queue, jobType)
	require.Equal(t, beforeS+1, testutil.ToFloat64(jobsSucceeded.WithLabelValues(queue, jobType)))

	beforeF := testutil.ToFloat64(jobsFailed.WithLabelValues(queue, jobType))
	RecordFailed(queue, jobType)
	require.Equal(t, beforeF+1, testutil.ToFloat64(jobsFailed.WithLabelValues(queue, jobType)))

	beforeD := testutil.ToFloat64(jobsDead.WithLabelValues(queue, jobType))
	RecordDead(queue, jobType)
	require.Equal(t, beforeD+1, testutil.ToFloat64(jobsDead.WithLabelValues(queue, jobType)))
}

func TestRecordJobDurationAndQueueWaitObserve(t *testing.T) {
	queue, jobType := "metrics-test-hist", "t1"

	before := histogramSampleCount(t, jobDuration, queue, jobType)
	RecordJobDuration(queue, jobType, 250*time.Millisecond)
	require.Equal(t, before+1, histogramSampleCount(t, jobDuration, queue, jobType))

	beforeWait := histogramSampleCount(t, jobQueueWait, queue, jobType)
	RecordQueueWait(queue, jobType, 3*time.Second)
	require.Equal(t, beforeWait+1, histogramSampleCount(t, jobQueueWait, queue, jobType))
}
