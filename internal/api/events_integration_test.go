//go:build integration

package api_test

import (
	"bufio"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/SIDEYS/jobqueue/internal/api"
	"github.com/SIDEYS/jobqueue/internal/events"
	"github.com/SIDEYS/jobqueue/internal/queue"
	"github.com/SIDEYS/jobqueue/internal/store"
)

func startPostgres(t *testing.T, ctx context.Context) string {
	t.Helper()

	ctr, err := postgres.Run(ctx, "postgres:16-alpine",
		postgres.WithDatabase("jobqueue"),
		postgres.WithUsername("jobqueue"),
		postgres.WithPassword("jobqueue"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, ctr.Terminate(context.Background()))
	})

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)
	return connStr
}

// TestSSEDeliversJobEnqueuedEvent exercises the whole chain end to end: a
// real database write publishes a NOTIFY, events.Listener picks it up over
// its own LISTEN connection, forwards it to the Hub, and a real HTTP
// client connected to GET /api/v1/events receives it as an SSE frame. Every
// piece of this has its own unit/integration coverage elsewhere (Hub
// backpressure, the notify choke point, Listener's reconnect behavior) -
// this is the one test that proves they're actually wired together
// correctly, not just individually correct.
func TestSSEDeliversJobEnqueuedEvent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(s.Close)

	hub := events.NewHub()
	t.Cleanup(hub.Close)

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	t.Cleanup(cancelListener)
	listener := events.NewListener(s.Pool(), hub, nil)
	go func() { _ = listener.Run(listenerCtx) }()

	q := queue.New(s)
	router := api.NewRouter(q, s, hub, nil)
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/api/v1/events", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// By the time Do() returns, the server has already written response
	// headers, which in the handler happens strictly after it calls
	// hub.Subscribe() - so this client is already registered with the hub
	// before the enqueue below fires. No extra synchronization needed.

	_, _, err = q.Enqueue(ctx, queue.EnqueueParams{
		Queue: "sse-test", JobType: "sleep", Payload: []byte(`{}`), MaxAttempts: 5,
	})
	require.NoError(t, err)

	lines := make(chan string, 100)
	go func() {
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		close(lines)
	}()

	var dataLine string
	timeout := time.After(15 * time.Second)
loop:
	for {
		select {
		case line, ok := <-lines:
			if !ok {
				t.Fatal("SSE stream closed before a data line arrived")
			}
			if strings.HasPrefix(line, "data: ") {
				dataLine = strings.TrimPrefix(line, "data: ")
				break loop
			}
		case <-timeout:
			t.Fatal("no SSE data line received within timeout")
		}
	}

	require.Contains(t, dataLine, `"status":"pending"`)
	require.Contains(t, dataLine, `"queue":"sse-test"`)
}
