package influx

import (
	"sync"
	"time"
)

type cacheKey struct {
	Addr        string
	DB          string
	Measurement string
}

var cache *LoadingCache[cacheKey, map[string]bool]

func init() {
	cache = NewLoadingCache[cacheKey, map[string]bool](24 * time.Hour)
}

type LoadingCache[K comparable, V any] struct {
	sync.RWMutex
	cache map[K]item[V]
	ttl   time.Duration
}

// cache value.
type item[V any] struct {
	value     V
	expiresAt time.Time
}

func NewLoadingCache[K comparable, V any](ttl time.Duration) *LoadingCache[K, V] {
	return &LoadingCache[K, V]{
		cache: make(map[K]item[V]),
		ttl:   ttl,
	}
}

func (c *LoadingCache[K, V]) Get(k K, loader func(k K) (V, error)) (V, error) {
	c.RLock()
	v, ok := c.cache[k]
	c.RUnlock()
	if ok && v.expiresAt.After(time.Now()) {
		return v.value, nil
	}

	c.Lock()
	defer c.Unlock()

	value, err := loader(k)
	if err != nil {
		return value, err
	}
	c.cache[k] = item[V]{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}

	return value, nil
}
