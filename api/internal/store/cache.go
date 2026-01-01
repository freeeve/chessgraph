package store

import "sync"

// simple LRU cache for ExpandedNode.
type cache[K comparable, V any] struct {
	cap   int
	mu    sync.Mutex
	order []K
	items map[K]V
}

func newCache[K comparable, V any](cap int) *cache[K, V] {
	return &cache[K, V]{cap: cap, items: make(map[K]V)}
}

func (c *cache[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	val, ok := c.items[key]
	if !ok {
		var zero V
		return zero, false
	}
	// move to front
	c.touchLocked(key)
	return val, true
}

func (c *cache[K, V]) Set(key K, val V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.items[key]; !ok && len(c.items) >= c.cap && c.cap > 0 {
		// evict LRU (tail)
		evict := c.order[len(c.order)-1]
		delete(c.items, evict)
		c.order = c.order[:len(c.order)-1]
	}
	c.items[key] = val
	c.touchLocked(key)
}

func (c *cache[K, V]) touchLocked(key K) {
	for i, k := range c.order {
		if k == key {
			copy(c.order[1:i+1], c.order[0:i])
			c.order[0] = key
			return
		}
	}
	c.order = append([]K{key}, c.order...)
}
