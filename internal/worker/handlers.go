package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

// SleepPayload is the payload for the "sleep" demo handler.
type SleepPayload struct {
	DurationMS int `json:"duration_ms"`
}

// SleepHandler sleeps for the requested duration and then succeeds. It
// exists so the deployed demo has a visible, controllable-duration job to
// watch move through pending -> running -> succeeded.
func SleepHandler(ctx context.Context, payload []byte) error {
	var p SleepPayload
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("sleep: invalid payload: %w", err)
		}
	}

	timer := time.NewTimer(time.Duration(p.DurationMS) * time.Millisecond)
	defer timer.Stop()

	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FlakyPayload is the payload for the "flaky" demo handler.
type FlakyPayload struct {
	FailRate float64 `json:"fail_rate"`
}

const defaultFlakyFailRate = 0.5

// FlakyHandler fails at random with the configured probability, so the
// demo has a visible source of retries and, eventually, dead-lettered
// jobs to look at.
func FlakyHandler(ctx context.Context, payload []byte) error {
	p := FlakyPayload{FailRate: defaultFlakyFailRate}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("flaky: invalid payload: %w", err)
		}
	}

	if rand.Float64() < p.FailRate {
		return fmt.Errorf("flaky: simulated failure (fail_rate=%.2f)", p.FailRate)
	}
	return nil
}

// RegisterDemoHandlers wires the built-in sleep/flaky handlers into a
// registry.
func RegisterDemoHandlers(r *Registry) {
	r.Register("sleep", SleepHandler)
	r.Register("flaky", FlakyHandler)
}
