package memorystore_test

import (
	"context"
	"testing"
	"time"

	"github.com/gfx-labs/narasu/src/storage/memorystore"
	"github.com/stretchr/testify/require"
)

func TestMemoryStore(t *testing.T) {
	ctx := context.Background()
	c := memorystore.NewStore(8)
	start := time.Unix(1000, 0)
	end := start.Add(time.Second * 14)

	bucket := "test"
	var val int
	var err error
	val, err = c.IncrKey(ctx, bucket, 1, start)
	require.NoError(t, err)
	require.Equal(t, 1, val)

	val, err = c.IncrKey(ctx, bucket, 1, start.Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, val)

	val, err = c.IncrKey(ctx, bucket, 1, start.Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, 2, val)

	val, err = c.IncrKey(ctx, bucket, 1, start.Add(time.Second*2))
	require.NoError(t, err)
	require.Equal(t, 1, val)

	val, err = c.IncrKey(ctx, bucket, 1, start.Add(time.Second*10))
	require.NoError(t, err)
	require.Equal(t, 1, val)

	val, err = c.IncrKey(ctx, bucket, 1, start.Add(time.Second*12))
	require.NoError(t, err)
	require.Equal(t, 1, val)

	xs, err := c.GetKeys(ctx, bucket, start, end)
	require.NoError(t, err)
	require.ElementsMatch(t, []int{
		1, 2, 1, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0,
	}, xs)

	err = c.Cleanup(ctx, end, 8*time.Second)
	require.NoError(t, err)

	xs, err = c.GetKeys(ctx, bucket, start, end)
	require.NoError(t, err)
	require.ElementsMatch(t, []int{
		0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0,
	}, xs)
}
