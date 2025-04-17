package redisstore_test

import (
	"context"
	"testing"
	"time"

	"github.com/gfx-labs/narasu/src/storage/redisstore"
	"github.com/gfx-labs/narasu/src/testutil"
	"github.com/stretchr/testify/require"
)

func TestRedisStore(t *testing.T) {
	ctx := context.Background()
	_, rc := testutil.GetMiniRedis(t)

	c, err := redisstore.NewStore(
		redisstore.WithRedis(rc),
	)
	require.NoError(t, err)

	start := time.Unix(1000, 0)
	end := start.Add(time.Second * 14)

	bucket := "test"
	var val int
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
