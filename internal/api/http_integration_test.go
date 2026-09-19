//go:build integration

package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/SIDEYS/jobqueue/internal/api"
	"github.com/SIDEYS/jobqueue/internal/events"
	"github.com/SIDEYS/jobqueue/internal/queue"
	"github.com/SIDEYS/jobqueue/internal/store"
)

// newTestServer wires a real store against a fresh Postgres container into
// a live httptest server, for endpoint-level integration tests that need
// the actual HTTP routing/JSON encoding, not just the store methods
// underneath them.
func newTestServer(t *testing.T, ctx context.Context) (*httptest.Server, *store.Store, *queue.Queue) {
	t.Helper()

	connStr := startPostgres(t, ctx)
	require.NoError(t, store.Migrate(connStr))

	s, err := store.New(ctx, connStr)
	require.NoError(t, err)
	t.Cleanup(s.Close)

	q := queue.New(s)
	hub := events.NewHub()
	t.Cleanup(hub.Close)

	router := api.NewRouter(q, s, hub, nil)
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	return srv, s, q
}

func getJSON(t *testing.T, url string, out any) *http.Response {
	t.Helper()
	resp, err := http.Get(url)
	require.NoError(t, err)
	defer resp.Body.Close()
	if out != nil {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
	}
	return resp
}

func TestHealthEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	srv, s, _ := newTestServer(t, ctx)

	resp, err := http.Get(srv.URL + "/healthz")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	resp, err = http.Get(srv.URL + "/readyz")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "readyz must be 200 while the database is reachable")

	// Once the pool is closed, readyz must flip to 503 - but healthz,
	// which checks nothing, must not change at all.
	s.Close()

	resp, err = http.Get(srv.URL + "/readyz")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	resp, err = http.Get(srv.URL + "/healthz")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "healthz must stay 200 regardless of database state")
}

func TestListJobsFilterAndPaginate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	srv, _, q := newTestServer(t, ctx)

	const total = 25
	for i := 0; i < total; i++ {
		_, _, err := q.Enqueue(ctx, queue.EnqueueParams{
			Queue: "list-test", JobType: "sleep", Payload: []byte(`{}`), MaxAttempts: 5,
		})
		require.NoError(t, err)
	}
	// A different queue that filtering must exclude.
	_, _, err := q.Enqueue(ctx, queue.EnqueueParams{
		Queue: "other-list-test", JobType: "sleep", Payload: []byte(`{}`), MaxAttempts: 5,
	})
	require.NoError(t, err)

	type listResp struct {
		Jobs       []map[string]any `json:"jobs"`
		NextCursor *string          `json:"next_cursor"`
	}

	seen := make(map[string]bool)
	url := fmt.Sprintf("%s/api/v1/jobs?queue=list-test&limit=7", srv.URL)
	pages := 0
	for {
		var page listResp
		resp := getJSON(t, url, &page)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		pages++

		for _, j := range page.Jobs {
			id := j["id"].(string)
			require.Falsef(t, seen[id], "job %s returned on more than one page", id)
			seen[id] = true
			require.Equal(t, "list-test", j["queue"], "filter must exclude other-list-test's job")
		}

		if page.NextCursor == nil {
			break
		}
		url = fmt.Sprintf("%s/api/v1/jobs?queue=list-test&limit=7&cursor=%s", srv.URL, *page.NextCursor)
		require.Lessf(t, pages, 10, "too many pages - pagination is probably not terminating")
	}

	require.Len(t, seen, total, "pagination must cover every matching job exactly once, no gaps or duplicates")
	require.Greater(t, pages, 1, "test is only meaningful if it actually spans multiple pages")
}

func TestQueueStatsEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	srv, _, q := newTestServer(t, ctx)

	for i := 0; i < 3; i++ {
		_, _, err := q.Enqueue(ctx, queue.EnqueueParams{
			Queue: "stats-test-a", JobType: "sleep", Payload: []byte(`{}`), MaxAttempts: 5,
		})
		require.NoError(t, err)
	}
	_, _, err := q.Enqueue(ctx, queue.EnqueueParams{
		Queue: "stats-test-b", JobType: "sleep", Payload: []byte(`{}`), MaxAttempts: 5,
	})
	require.NoError(t, err)

	var got struct {
		Queues []struct {
			Queue  string           `json:"queue"`
			Counts map[string]int64 `json:"counts"`
		} `json:"queues"`
	}
	resp := getJSON(t, srv.URL+"/api/v1/queues", &got)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	byQueue := make(map[string]map[string]int64)
	for _, q := range got.Queues {
		byQueue[q.Queue] = q.Counts
	}
	require.Equal(t, int64(3), byQueue["stats-test-a"]["pending"])
	require.Equal(t, int64(1), byQueue["stats-test-b"]["pending"])
}

func TestListWorkersEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	srv, s, _ := newTestServer(t, ctx)

	require.NoError(t, s.UpsertHeartbeat(ctx, "worker-http-test", "host-x", 2))

	var got struct {
		Workers []struct {
			ID           string `json:"id"`
			JobsInFlight int32  `json:"jobs_in_flight"`
		} `json:"workers"`
	}
	resp := getJSON(t, srv.URL+"/api/v1/workers", &got)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.Len(t, got.Workers, 1)
	require.Equal(t, "worker-http-test", got.Workers[0].ID)
	require.Equal(t, int32(2), got.Workers[0].JobsInFlight)
}

func TestThroughputEndpoint(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	srv, _, q := newTestServer(t, ctx)

	_, _, err := q.Enqueue(ctx, queue.EnqueueParams{
		Queue: "throughput-test", JobType: "sleep", Payload: []byte(`{}`), MaxAttempts: 5,
	})
	require.NoError(t, err)
	claimed, err := q.Claim(ctx, "throughput-test", 1, "throughput-worker")
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.NoError(t, q.Complete(ctx, claimed[0]))

	var got struct {
		Buckets []struct {
			Minute    string `json:"minute"`
			Succeeded int64  `json:"succeeded"`
		} `json:"buckets"`
	}
	resp := getJSON(t, srv.URL+"/api/v1/stats/throughput", &got)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.Len(t, got.Buckets, 61, "one bucket per minute across a full hour window, inclusive of both ends")

	var total int64
	for _, b := range got.Buckets {
		total += b.Succeeded
	}
	require.Equal(t, int64(1), total, "the one completed job must appear exactly once, in whichever minute bucket it landed in")

	completedAt, err := time.Parse(time.RFC3339, got.Buckets[len(got.Buckets)-1].Minute)
	require.NoError(t, err)
	require.WithinDuration(t, time.Now().UTC(), completedAt, time.Minute, "the job completed just now, so it must land in the most recent bucket")
}

func TestScheduleCRUDEndpoints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	srv, _, _ := newTestServer(t, ctx)

	// Invalid cron is rejected before touching the database.
	resp, err := http.Post(srv.URL+"/api/v1/schedules", "application/json",
		strings.NewReader(`{"queue":"sched-test","job_type":"sleep","cron_expr":"nonsense"}`))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	created := strings.NewReader(`{"queue":"sched-test","job_type":"sleep","cron_expr":"*/5 * * * *","payload":{"n":1}}`)
	resp, err = http.Post(srv.URL+"/api/v1/schedules", "application/json", created)
	require.NoError(t, err)
	var sc struct {
		ID      string `json:"id"`
		Enabled bool   `json:"enabled"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&sc))
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.True(t, sc.Enabled)

	// Get.
	resp = getJSON(t, srv.URL+"/api/v1/schedules/"+sc.ID, &sc)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	// List includes it.
	var listed struct {
		Schedules []struct {
			ID string `json:"id"`
		} `json:"schedules"`
	}
	resp = getJSON(t, srv.URL+"/api/v1/schedules", &listed)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	found := false
	for _, s := range listed.Schedules {
		if s.ID == sc.ID {
			found = true
		}
	}
	require.True(t, found, "created schedule must appear in the list")

	// Patch: disable.
	req, err := http.NewRequest(http.MethodPatch, srv.URL+"/api/v1/schedules/"+sc.ID, strings.NewReader(`{"enabled":false}`))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&sc))
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.False(t, sc.Enabled)

	// Delete.
	req, err = http.NewRequest(http.MethodDelete, srv.URL+"/api/v1/schedules/"+sc.ID, nil)
	require.NoError(t, err)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)

	// Gone.
	resp, err = http.Get(srv.URL + "/api/v1/schedules/" + sc.ID)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
