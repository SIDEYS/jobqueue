// Package events fans job state-change notifications out to connected SSE
// clients. It is not a source of truth: delivery is at-most-once, and a
// client that was disconnected has simply missed whatever happened while
// it was gone. See docs/ARCHITECTURE.md for why that's an intentional
// property of this stream, not a gap to close.
package events

// Event is what a connected client receives - deliberately the same
// minimal shape published over Postgres NOTIFY (see
// internal/store/notify.go): enough to know something changed, not what
// it changed to in detail.
type Event struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Queue  string `json:"queue"`
}

// eventBufferSize is how many events a client can be behind before Hub
// gives up on it. Small on purpose: a client that's behind by this many
// job-state transitions is not keeping up in any real sense, and holding
// a deep backlog for it just delays the moment everyone else would notice
// the same problem.
const eventBufferSize = 16

// Hub is a simple in-process pub/sub for Event. The zero value is not
// usable; construct with NewHub.
type Hub struct {
	register   chan chan Event
	unregister chan chan Event
	broadcast  chan Event
	done       chan struct{}
}

func NewHub() *Hub {
	h := &Hub{
		register:   make(chan chan Event),
		unregister: make(chan chan Event),
		broadcast:  make(chan Event),
		done:       make(chan struct{}),
	}
	go h.run()
	return h
}

// run owns the client set on a single goroutine, so Subscribe/
// Unsubscribe/Broadcast never need their own locking - every mutation of
// the set happens here, serialized through these three channels.
func (h *Hub) run() {
	clients := make(map[chan Event]struct{})
	for {
		select {
		case ch := <-h.register:
			clients[ch] = struct{}{}
		case ch := <-h.unregister:
			if _, ok := clients[ch]; ok {
				delete(clients, ch)
				close(ch)
			}
		case evt := <-h.broadcast:
			for ch := range clients {
				select {
				case ch <- evt:
				default:
					// This client's buffer is full - it's not keeping up.
					// Drop the client, not the event for everyone else: a
					// disconnected client just reconnects and re-fetches
					// current state (see ARCHITECTURE.md), which recovers
					// cleanly. A client that stayed "connected" but
					// silently missed events would look fine while quietly
					// lying to whoever's watching it.
					delete(clients, ch)
					close(ch)
				}
			}
		case <-h.done:
			for ch := range clients {
				close(ch)
			}
			return
		}
	}
}

// Subscribe registers a new client and returns the channel it will
// receive events on. The caller must eventually call Unsubscribe with the
// same channel - even if Hub already closed it due to backpressure,
// Unsubscribe is safe to call regardless (see Unsubscribe).
func (h *Hub) Subscribe() chan Event {
	ch := make(chan Event, eventBufferSize)
	select {
	case h.register <- ch:
	case <-h.done:
		close(ch)
	}
	return ch
}

// Unsubscribe removes ch from the client set, if it's still registered.
// Safe to call even if Hub already dropped and closed ch itself (via
// backpressure in Broadcast) - run's ownership of the client set means
// there's no race between that and this, so it's simply a no-op in that
// case rather than a double-close panic.
func (h *Hub) Unsubscribe(ch chan Event) {
	select {
	case h.unregister <- ch:
	case <-h.done:
	}
}

// Broadcast publishes evt to every currently-subscribed client.
func (h *Hub) Broadcast(evt Event) {
	select {
	case h.broadcast <- evt:
	case <-h.done:
	}
}

// Close stops the hub and closes every currently-subscribed client's
// channel.
func (h *Hub) Close() {
	close(h.done)
}
