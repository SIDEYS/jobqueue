package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPollIntervalDoublesUntilCapped(t *testing.T) {
	base := 500 * time.Millisecond
	max := 2 * time.Second

	require.Equal(t, 500*time.Millisecond, pollInterval(1, base, max))
	require.Equal(t, 1*time.Second, pollInterval(2, base, max))
	require.Equal(t, 2*time.Second, pollInterval(3, base, max))
	// 4th doubling (4s) would exceed max; must stay capped.
	require.Equal(t, 2*time.Second, pollInterval(4, base, max))
	require.Equal(t, 2*time.Second, pollInterval(100, base, max))
}

func TestPollIntervalNonPositiveStreakIsBase(t *testing.T) {
	base := 500 * time.Millisecond
	max := 2 * time.Second

	require.Equal(t, base, pollInterval(0, base, max))
	require.Equal(t, base, pollInterval(-3, base, max))
}
