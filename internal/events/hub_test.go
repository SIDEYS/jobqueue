package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHubDeliversToASingleSubscriber(t *testing.T) {
	h := NewHub()
	defer h.Close()

	ch := h.Subscribe()
	defer h.Unsubscribe(ch)

	h.Broadcast(Event{ID: "1", Status: "succeeded", Queue: "default"})

	select {
	case evt := <-ch:
		require.Equal(t, Event{ID: "1", Status: "succeeded", Queue: "default"}, evt)
	case <-time.After(2 * time.Second):
		t.Fatal("event never arrived")
	}
}

// floodToDrop sends enough broadcasts to fill ch's buffer and force Hub to
// drop it, then sends one further broadcast before returning.
//
// That trailing extra broadcast matters for determinism: Broadcast(evt)
// only blocks until run() has *received* evt, not until run() has finished
// deciding what to do with it (buffer it or drop the client) - that
// decision happens later in the same iteration of run()'s loop, after the
// channel send has already unblocked the caller. So the very last
// broadcast in a flood can still be mid-decision in run() at the moment
// this function returns, and a concurrent reader draining the buffer at
// exactly that moment can "rescue" it into being buffered instead of
// dropped, racily. Every broadcast *before* the last one doesn't have
// this problem: run() can't receive broadcast N+1 until it has looped
// back to select, which requires broadcast N's full handling - including
// the drop decision - to have already finished. So one harmless trailing
// broadcast after the one that fills the buffer is enough to guarantee,
// by the time this function returns, that the fill-triggering broadcast's
// fate has already been decided.
func floodToDrop(h *Hub) {
	for i := 0; i < eventBufferSize+2; i++ {
		h.Broadcast(Event{ID: "flood", Status: "x", Queue: "q"})
	}
}

// drainUntilClosed blocks-reads from ch (not polling) until it observes
// the channel closed, or fails the test if timeout elapses first. A
// blocking read proceeds the instant the next buffered item is available,
// so - unlike require.Eventually on a fixed polling interval - draining a
// channel with many backlogged items isn't gated by how many poll
// intervals fit in the overall timeout under heavy scheduler contention.
func drainUntilClosed(t *testing.T, ch <-chan Event, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case _, open := <-ch:
			if !open {
				return
			}
		case <-deadline:
			t.Fatal("channel was not closed within timeout")
		}
	}
}

// TestHubDropsASlowClientInsteadOfBlockingOrDroppingForOthers deliberately
// doesn't try to keep a second "fast" client concurrently draining while
// the flood is in flight - an earlier version did, using a background
// goroutine, and it was racy: that goroutine's first scheduling isn't
// guaranteed to happen before the flood starts, so under enough scheduler
// contention (many concurrent tests, race detector overhead) the "fast"
// client could just as easily overflow and get dropped too, same as slow,
// non-deterministically. That risk is a property of the test's own
// construction, not of Hub: Broadcast's per-client send is a non-blocking
// `select { case ch <- evt: default: drop }`, so one client's buffer state
// can structurally never delay or corrupt delivery to another - there's no
// shared state between two clients' cases for one to block the other on.
// What's actually worth proving here is the part that isn't already
// obvious from reading the code: that dropping a client leaves the hub's
// internal bookkeeping consistent, so broadcast keeps working normally for
// everyone else afterward.
func TestHubDropsASlowClientInsteadOfBlockingOrDroppingForOthers(t *testing.T) {
	h := NewHub()
	defer h.Close()

	slow := h.Subscribe()
	floodToDrop(h)

	// slow must have been dropped: its channel is closed. floodToDrop
	// already guarantees the drop decision is made by the time it
	// returns; this just drains whatever backlog was buffered before the
	// drop to reach the close.
	drainUntilClosed(t, slow, 5*time.Second)

	// A client subscribed after the drop must receive broadcasts
	// normally - the drop must not have left the hub's client set (or
	// anything else internal) corrupted.
	fresh := h.Subscribe()
	defer h.Unsubscribe(fresh)

	h.Broadcast(Event{ID: "after-drop", Status: "succeeded", Queue: "default"})
	select {
	case evt := <-fresh:
		require.Equal(t, "after-drop", evt.ID)
	case <-time.After(5 * time.Second):
		t.Fatal("a client subscribed after the drop never received a subsequent broadcast")
	}
}

func TestUnsubscribeIsSafeAfterHubAlreadyDroppedTheClient(t *testing.T) {
	h := NewHub()
	defer h.Close()

	ch := h.Subscribe()
	floodToDrop(h)

	drainUntilClosed(t, ch, 5*time.Second)

	// Must not panic (double-close) even though the hub already closed ch.
	require.NotPanics(t, func() { h.Unsubscribe(ch) })
}
