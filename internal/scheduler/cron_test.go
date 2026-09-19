package scheduler

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNextRunDailyAtMidnight(t *testing.T) {
	after := time.Date(2026, 3, 1, 13, 45, 0, 0, time.UTC)
	next, err := NextRun("0 0 * * *", after)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 3, 2, 0, 0, 0, 0, time.UTC), next)
	require.Equal(t, time.UTC, next.Location())
}

func TestNextRunEveryFiveMinutes(t *testing.T) {
	after := time.Date(2026, 3, 1, 13, 42, 0, 0, time.UTC)
	next, err := NextRun("*/5 * * * *", after)
	require.NoError(t, err)
	require.Equal(t, time.Date(2026, 3, 1, 13, 45, 0, 0, time.UTC), next)
}

func TestNextRunIsAlwaysStrictlyAfterReference(t *testing.T) {
	// Reference time lands exactly on a firing instant - Next must return
	// the *next* one, not the same instant back.
	after := time.Date(2026, 3, 1, 13, 45, 0, 0, time.UTC)
	next, err := NextRun("*/5 * * * *", after)
	require.NoError(t, err)
	require.True(t, next.After(after))
	require.Equal(t, time.Date(2026, 3, 1, 13, 50, 0, 0, time.UTC), next)
}

func TestNextRunInvalidExpression(t *testing.T) {
	_, err := NextRun("not a cron expression", time.Now())
	require.Error(t, err)
}

func TestNextRunForcesUTCRegardlessOfInputLocation(t *testing.T) {
	// A daily-at-2:30 schedule evaluated with a reference time carrying a
	// non-UTC offset must still be computed against UTC calendar fields,
	// not the offset's local calendar fields - this is what actually
	// avoids the DST double-fire/skip problem described in the schedules
	// migration.
	weirdZone := time.FixedZone("UTC+5:30", 5*3600+30*60)
	afterInWeirdZone := time.Date(2026, 3, 1, 19, 0, 0, 0, weirdZone) // = 13:30 UTC
	afterInUTC := afterInWeirdZone.UTC()
	require.Equal(t, time.Date(2026, 3, 1, 13, 30, 0, 0, time.UTC), afterInUTC)

	nextFromWeird, err := NextRun("30 2 * * *", afterInWeirdZone)
	require.NoError(t, err)
	nextFromUTC, err := NextRun("30 2 * * *", afterInUTC)
	require.NoError(t, err)

	require.Equal(t, nextFromUTC, nextFromWeird)
	require.Equal(t, time.UTC, nextFromWeird.Location())
}
