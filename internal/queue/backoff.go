package queue

import (
	"math/rand"
	"time"
)

const (
	backoffBase = 1 * time.Second
	backoffCap  = 5 * time.Minute
	// maxShift bounds the exponent so backoffBase * 2^shift can never
	// overflow time.Duration (an int64 count of nanoseconds) even before
	// backoffCap is applied - 2^32 seconds alone is already far past the
	// cap, so this never changes real behavior, it just keeps the
	// intermediate arithmetic safe.
	maxShift = 32
)

// Backoff computes how long to wait before retrying a job that has failed
// attempt times, using exponential backoff with full jitter:
//
//	sleep = random_between(0, min(backoffCap, backoffBase * 2^(attempt-1)))
//
// Full jitter - randomizing the entire sleep, not just adding noise to a
// fixed delay - matters because every job that failed for the same reason
// at the same attempt count would otherwise wake up and retry at exactly
// the same instant. If a downstream dependency caused the failures, that
// produces a synchronized retry storm (thundering herd) against a system
// that's already struggling. Randomizing the actual wait spreads retries
// out across the whole window instead of re-synchronizing them a moment
// later.
//
// rng is passed in rather than using the package-level math/rand source so
// this stays a pure function: the same (attempt, rng state) always
// produces the same result, which is what makes it testable without
// sleeping in the test itself.
func Backoff(attempt int, rng *rand.Rand) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > maxShift {
		shift = maxShift
	}

	exp := backoffBase * time.Duration(int64(1)<<uint(shift))
	capped := exp
	if capped > backoffCap || capped <= 0 {
		capped = backoffCap
	}

	return time.Duration(rng.Int63n(int64(capped) + 1))
}
