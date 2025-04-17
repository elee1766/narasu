package narasu_test

import (
	"context"
	"testing"
	"time"

	"github.com/gfx-labs/narasu"
	"github.com/gfx-labs/narasu/src/storage/memorystore"
	"github.com/gfx-labs/narasu/src/storage/redisstore"
	"github.com/gfx-labs/narasu/src/testutil"
	"github.com/stretchr/testify/require"
)

func TestMemoryStore(t *testing.T) {
	testStore(t, memorystore.NewStore(8))
}

func TestRedisStore(t *testing.T) {
	_, rc := testutil.GetMiniRedis(t)
	c, err := redisstore.NewStore(
		redisstore.WithRedis(rc),
	)
	require.NoError(t, err)
	testStore(t, c)
}

func testStore(t *testing.T, store narasu.Store) {
	ctx := context.Background()
	c, err := narasu.NewClient(
		narasu.WithStore(store),
		narasu.WithMaxOldAge(8*time.Second),
	)
	require.NoError(t, err)
	start := time.Unix(1000, 0)
	end := start.Add(time.Second * 15)

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

	val, err = c.Window(ctx, bucket, start, end)
	require.NoError(t, err)
	require.Equal(t, 6, val)

	err = c.Cleanup(ctx, end)
	require.NoError(t, err)

	val, err = c.Window(ctx, bucket, start, end)
	require.NoError(t, err)
	require.Equal(t, 2, val)
}
