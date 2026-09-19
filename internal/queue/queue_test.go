package queue

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnqueueValidation(t *testing.T) {
	q := New(nil)

	_, _, err := q.Enqueue(context.Background(), EnqueueParams{JobType: "sleep"})
	require.ErrorContains(t, err, "queue name is required")

	_, _, err = q.Enqueue(context.Background(), EnqueueParams{Queue: "default"})
	require.ErrorContains(t, err, "job_type is required")
}
