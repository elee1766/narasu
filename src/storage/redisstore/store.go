package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/rueidis"
)

type Store struct {
	r         rueidis.CoreClient
	namespace string
	ttl       time.Duration
}

type StoreOption func(s *Store) *Store

func WithRedis(r rueidis.CoreClient) StoreOption {
	return func(s *Store) *Store {
		s.r = r
		return s
	}
}

func WithNamespace(namespace string) StoreOption {
	return func(s *Store) *Store {
		s.namespace = namespace
		return s
	}
}

func WithTTL(ttl time.Duration) StoreOption {
	return func(s *Store) *Store {
		s.ttl = ttl
		return s
	}
}

func NewStore(
	options ...StoreOption,
) (*Store, error) {
	d := &Store{}

	for _, option := range options {
		if option == nil {
			continue
		}
		option(d)
	}
	if d.r == nil {
		return nil, fmt.Errorf("redis client is required for redisstore")
	}

	return d, nil
}

func (s *Store) prefix() string {
	if s.namespace == "" {
		return "narasu:buckets:"
	}
	return s.namespace + ":narasu:buckets:"
}
func (s *Store) makeTimeKey(cur time.Time) string {
	return s.prefix() + strconv.Itoa(int(cur.Unix()))
}
func (s *Store) makeKey(cur time.Time, bucket string) string {
	return s.makeTimeKey(cur) + ":{" + bucket + "}"
}

func (s *Store) IncrKey(ctx context.Context, bucket string, amount int, cur time.Time) (int, error) {
	key := s.makeKey(cur, bucket)
	newVal, err := s.r.Do(ctx, s.r.B().Incrby().Key(key).Increment(int64(amount)).Build()).ToInt64()
	if err != nil {
		return 0, err
	}
	if int(newVal) == amount && s.ttl > 0 {
		// set expiration
		err = s.r.Do(ctx, s.r.B().Expireat().Key(key).Timestamp(int64(
			cur.Add(s.ttl).Unix(),
		)).Build()).Error()
		if err != nil {
			return 0, err
		}
	}
	return int(newVal), nil
}

func (s *Store) GetKeys(ctx context.Context, bucket string, from time.Time, to time.Time) ([]int, error) {
	keys := make([]string, 0)
	cmd := s.r.B().Mget()
	for i := from; i.Compare(to) <= 0; i = i.Add(time.Second) {
		key := s.makeKey(i, bucket)
		keys = append(keys, key)
	}
	xs, err := s.r.Do(ctx, cmd.Key(keys...).Build()).AsIntSlice()
	if err != nil {
		return nil, err
	}
	out := make([]int, len(xs))
	for i, x := range xs {
		out[i] = int(x)
	}
	return out, nil
}

func (s *Store) Cleanup(ctx context.Context, now time.Time, age time.Duration) error {
	var expired []string
	nowseconds := int(now.Unix())
	ageSeconds := int(age.Seconds())
	var cur rueidis.ScanEntry
	var err error
	count := 1024
	for {
		match := s.prefix() + "*"
		cur, err = s.r.Do(ctx, s.r.B().
			Scan().
			Cursor(cur.Cursor).Match(match).
			Count(int64(count)).
			Build()).AsScanEntry()
		if err != nil {
			return err
		}
		for _, v := range cur.Elements {
			// extract timestamp from prefix:timestamp:{bucket}
			prefix := s.prefix()
			if !strings.HasPrefix(v, prefix) {
				continue
			}
			noPrefix := strings.TrimPrefix(v, prefix)
			parts := strings.SplitN(noPrefix, ":", 2)
			if len(parts) != 2 {
				continue
			}
			timestamp, err := strconv.Atoi(parts[0])
			if err != nil {
				continue
			}
			if nowseconds-timestamp > ageSeconds {
				expired = append(expired, v)
			}
		}
		if len(cur.Elements) != count {
			break
		}
	}
	if len(expired) > 0 {
		// delete them
		err = s.r.Do(ctx, s.r.B().Del().Key(expired...).Build()).Error()
		if err != nil {
			return err
		}
	}
	return nil
}
