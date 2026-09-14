package queue

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// expectedCap mirrors the formula in Backoff for use as a test oracle,
// deliberately re-derived rather than imported so the test can't pass just
// because it shares a bug with the implementation.
func expectedCap(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 62 { // avoid overflowing the shift itself in the test's own math
		shift = 62
	}
	exp := backoffBase * time.Duration(int64(1)<<uint(shift))
	if exp > backoffCap || exp <= 0 {
		return backoffCap
	}
	return exp
}

func TestBackoffStaysWithinExpectedCapAtEachAttempt(t *testing.T) {
	rng := rand.New(rand.NewSource(1))

	for _, attempt := range []int{1, 2, 3, 4, 9, 10, 20, 40} {
		wantCap := expectedCap(attempt)
		for i := 0; i < 500; i++ {
			d := Backoff(attempt, rng)
			require.GreaterOrEqualf(t, d, time.Duration(0), "attempt=%d", attempt)
			require.LessOrEqualf(t, d, wantCap, "attempt=%d", attempt)
		}
	}
}

func TestBackoffCapsAtBackoffCapForLargeAttempts(t *testing.T) {
	rng := rand.New(rand.NewSource(2))
	for i := 0; i < 500; i++ {
		d := Backoff(1000, rng)
		require.LessOrEqual(t, d, backoffCap)
	}
}

func TestBackoffTreatsNonPositiveAttemptAsOne(t *testing.T) {
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < 500; i++ {
		d := Backoff(0, rng)
		require.LessOrEqual(t, d, backoffBase)

		d = Backoff(-5, rng)
		require.LessOrEqual(t, d, backoffBase)
	}
}

func TestBackoffIsJittered(t *testing.T) {
	rng := rand.New(rand.NewSource(4))
	seen := make(map[time.Duration]bool)
	for i := 0; i < 50; i++ {
		seen[Backoff(5, rng)] = true
	}
	require.Greater(t, len(seen), 1, "expected varying delays across calls, got all identical values")
}

func TestBackoffIsDeterministicForAGivenRNGState(t *testing.T) {
	a := Backoff(5, rand.New(rand.NewSource(42)))
	b := Backoff(5, rand.New(rand.NewSource(42)))
	require.Equal(t, a, b)
}
