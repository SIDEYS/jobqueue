package worker

import "time"

// pollInterval computes how long to wait before the next claim attempt
// after emptyStreak consecutive empty claims, doubling from base and
// capped at max.
//
// No jitter here, unlike queue.Backoff: that backoff exists to desynchronize
// many callers retrying against a shared, possibly-struggling dependency.
// This is a single worker's own idle polling cadence - there's no shared
// resource for synchronized wake-ups to overwhelm, just this process
// deciding how often to ask "is there work yet."
//
// The cap matters more than the growth curve: a large max poll interval
// trades idle-time DB load for latency on the next job enqueued after the
// queue goes quiet - a job enqueued right after a claim finds an interval
// backed off to, say, 5s waits up to 5s before any worker asks again. Kept
// small (WORKER_MAX_POLL_INTERVAL, 2s by default) so that latency floor
// stays low; the real fix for polling latency entirely is LISTEN/NOTIFY
// (see README limitations).
func pollInterval(emptyStreak int, base, max time.Duration) time.Duration {
	if emptyStreak < 1 {
		return base
	}
	shift := emptyStreak - 1
	if shift > 32 { // guard against overflowing time.Duration's int64 nanoseconds
		shift = 32
	}
	d := base * time.Duration(int64(1)<<uint(shift))
	if d > max || d <= 0 {
		return max
	}
	return d
}
