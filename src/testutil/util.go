package testutil

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func GetMiniRedis(t *testing.T) (*miniredis.Miniredis, rueidis.Client) {
	mr := miniredis.RunT(t)
	parsed, err := rueidis.ParseURL("redis://" + mr.Addr())
	parsed.DisableCache = true

	rc, err := rueidis.NewClient(parsed)
	require.NoError(t, err)
	return mr, rc
}
