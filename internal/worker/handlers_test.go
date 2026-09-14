package worker

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSleepHandlerSleepsAndSucceeds(t *testing.T) {
	start := time.Now()
	err := SleepHandler(context.Background(), []byte(`{"duration_ms": 20}`))
	require.NoError(t, err)
	require.GreaterOrEqual(t, time.Since(start), 20*time.Millisecond)
}

func TestSleepHandlerRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := SleepHandler(ctx, []byte(`{"duration_ms": 5000}`))
	require.ErrorIs(t, err, context.Canceled)
}

func TestFlakyHandlerAlwaysFailsAtRateOne(t *testing.T) {
	err := FlakyHandler(context.Background(), []byte(`{"fail_rate": 1}`))
	require.Error(t, err)
}

func TestFlakyHandlerNeverFailsAtRateZero(t *testing.T) {
	err := FlakyHandler(context.Background(), []byte(`{"fail_rate": 0}`))
	require.NoError(t, err)
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	_, ok := r.Get("sleep")
	require.False(t, ok)

	RegisterDemoHandlers(r)
	h, ok := r.Get("sleep")
	require.True(t, ok)
	require.NotNil(t, h)
}
