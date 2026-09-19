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

	// Fill slow's buffer without ever reading it, then push one more than
	// it can hold.
	for i := 0; i < eventBufferSize+1; i++ {
		h.Broadcast(Event{ID: "flood", Status: "x", Queue: "q"})
	}

	// slow must have been dropped: its channel is closed.
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
	for i := 0; i < eventBufferSize+1; i++ {
		h.Broadcast(Event{ID: "flood", Status: "x", Queue: "q"})
	}

	require.Eventually(t, func() bool {
		_, open := <-ch
		return !open
	}, 2*time.Second, 10*time.Millisecond, "hub should have dropped and closed the flooded client")

	// Must not panic (double-close) even though the hub already closed ch.
	require.NotPanics(t, func() { h.Unsubscribe(ch) })
}
