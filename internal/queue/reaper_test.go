package queue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeClock struct {
	now time.Time
}

func (f fakeClock) Now() time.Time { return f.now }

func TestReaperThresholdIsNowMinusVisibilityTimeout(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	r := NewReaper(nil, 90*time.Second).WithClock(fakeClock{now: now})

	require.Equal(t, now.Add(-90*time.Second), r.Threshold())
}

func TestReaperThresholdMovesWithTheInjectedClock(t *testing.T) {
	r := NewReaper(nil, time.Minute).WithClock(fakeClock{now: time.Unix(1000, 0)})
	first := r.Threshold()

	r.WithClock(fakeClock{now: time.Unix(2000, 0)})
	second := r.Threshold()

	require.True(t, second.After(first))
	require.Equal(t, time.Unix(940, 0), first)
	require.Equal(t, time.Unix(1940, 0), second)
}
