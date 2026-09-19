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

func TestHubDropsASlowClientInsteadOfBlockingOrDroppingForOthers(t *testing.T) {
	h := NewHub()
	defer h.Close()

	slow := h.Subscribe()
	fast := h.Subscribe()
	defer h.Unsubscribe(fast)

	// fast actually keeps up in real time, draining as events arrive -
	// this is what makes it "fast" rather than just another unread
	// buffer. Without this it would fill and get dropped too, same as
	// slow, and the test would prove nothing about the two being treated
	// differently.
	fastReceived := make(chan Event, 10000)
	go func() {
		for evt := range fast {
			fastReceived <- evt
		}
	}()

	floodToDrop(h)

	// slow must have been dropped: its channel is closed. (Draining here
	// is safe even though floodToDrop already guarantees the drop
	// happened - any buffered items read first are just leftover flood
	// events, and the loop only stops once it hits the close.)
	require.Eventually(t, func() bool {
		select {
		case _, open := <-slow:
			return !open
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "hub must close a dropped client's channel")

	// fast must still be receiving - a slow client must not stall
	// broadcast for everyone else.
	h.Broadcast(Event{ID: "after-drop", Status: "succeeded", Queue: "default"})
	require.Eventually(t, func() bool {
		select {
		case evt := <-fastReceived:
			return evt.ID == "after-drop"
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "fast client never received the post-drop event")
}

func TestUnsubscribeIsSafeAfterHubAlreadyDroppedTheClient(t *testing.T) {
	h := NewHub()
	defer h.Close()

	ch := h.Subscribe()
	floodToDrop(h)

	require.Eventually(t, func() bool {
		select {
		case _, open := <-ch:
			return !open
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond, "hub should have dropped and closed the flooded client")

	// Must not panic (double-close) even though the hub already closed ch.
	require.NotPanics(t, func() { h.Unsubscribe(ch) })
}
