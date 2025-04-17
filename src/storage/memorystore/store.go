package memorystore

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/btree"
)

func makeKey(cur time.Time, bucket string) string {
	return strconv.Itoa(int(cur.Unix())) + ":" + bucket
}

type Store struct {
	mu   sync.RWMutex
	tree *btree.Map[string, int]
}

func NewStore(degree int) *Store {
	c := &Store{
		tree: btree.NewMap[string, int](degree),
	}
	return c
}

func (s *Store) IncrKey(ctx context.Context, bucket string, amount int, cur time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := makeKey(cur, bucket)
	val, ok := s.tree.Get(key)
	if !ok {
		val = 0
	}
	newVal := val + amount
	s.tree.Set(key, newVal)
	return newVal, nil
}

func (s *Store) GetKeys(ctx context.Context, bucket string, from time.Time, to time.Time) ([]int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]int, 0)
	for i := from; i.Compare(to) <= 0; i = i.Add(time.Second) {
		key := makeKey(i, bucket)
		val, ok := s.tree.Get(key)
		if !ok {
			val = 0
		}
		out = append(out, val)
	}
	return out, nil
}

func (s *Store) Cleanup(ctx context.Context, now time.Time, age time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var expired []string
	nowseconds := int(now.Unix())
	ageSeconds := int(age.Seconds())
	s.tree.Ascend("", func(key string, val int) bool {
		// extract the timestamp from the key timestamp:bucket
		ts, err := strconv.Atoi(key[:strings.Index(key, ":")])
		if err != nil {
			log.Panicln("illegal key in map", key)
		}
		if nowseconds-ts > ageSeconds {
			expired = append(expired, key)
		}
		return true
	})
	for _, key := range expired {
		s.tree.Delete(key)
	}
	return nil
}
