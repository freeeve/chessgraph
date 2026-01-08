package store

import (
	"sync"
	"sync/atomic"
)

// PositionCache is a sharded LRU cache for position records.
// It provides O(1) lookups with minimal lock contention by sharding
// based on the first byte of the key.
type PositionCache struct {
	shards      [256]*positionCacheShard
	maxPerShard int
	hits        uint64
	misses      uint64
}

type positionCacheShard struct {
	mu    sync.RWMutex
	cache map[[KeySize]byte]PositionRecord
	order [][KeySize]byte // FIFO order for eviction
}

// NewPositionCache creates a new position cache with the given maximum size.
// Size is in bytes; each entry uses approximately 84 bytes.
func NewPositionCache(maxBytes int64) *PositionCache {
	// Estimate bytes per entry: 26 (key) + 14 (value) + ~50 (map overhead) = ~90
	const bytesPerEntry = 90
	maxEntries := int(maxBytes / bytesPerEntry)
	maxPerShard := maxEntries / 256
	if maxPerShard < 100 {
		maxPerShard = 100 // minimum per shard
	}

	pc := &PositionCache{
		maxPerShard: maxPerShard,
	}

	for i := range pc.shards {
		pc.shards[i] = &positionCacheShard{
			cache: make(map[[KeySize]byte]PositionRecord),
			order: make([][KeySize]byte, 0, maxPerShard),
		}
	}

	return pc
}

// Get retrieves a position from the cache. Returns nil if not found.
func (pc *PositionCache) Get(key [KeySize]byte) *PositionRecord {
	shard := pc.shards[key[0]]

	shard.mu.RLock()
	rec, ok := shard.cache[key]
	shard.mu.RUnlock()

	if ok {
		atomic.AddUint64(&pc.hits, 1)
		return &rec
	}
	atomic.AddUint64(&pc.misses, 1)
	return nil
}

// Put adds or updates a position in the cache.
func (pc *PositionCache) Put(key [KeySize]byte, rec *PositionRecord) {
	shard := pc.shards[key[0]]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Check if already exists
	if _, exists := shard.cache[key]; exists {
		// Update existing entry (don't add to order again)
		shard.cache[key] = *rec
		return
	}

	// Evict if at capacity (FIFO - evict oldest)
	for len(shard.cache) >= pc.maxPerShard && len(shard.order) > 0 {
		oldest := shard.order[0]
		shard.order = shard.order[1:]
		delete(shard.cache, oldest)
	}

	// Add new entry
	shard.cache[key] = *rec
	shard.order = append(shard.order, key)
}

// Invalidate removes a position from the cache.
func (pc *PositionCache) Invalidate(key [KeySize]byte) {
	shard := pc.shards[key[0]]

	shard.mu.Lock()
	defer shard.mu.Unlock()

	delete(shard.cache, key)
	// Note: we don't remove from order slice to avoid O(n) scan.
	// The key will be skipped during eviction if not in cache.
}

// Stats returns cache statistics.
func (pc *PositionCache) Stats() (hits, misses uint64, size int, capacity int) {
	hits = atomic.LoadUint64(&pc.hits)
	misses = atomic.LoadUint64(&pc.misses)

	for _, shard := range pc.shards {
		shard.mu.RLock()
		size += len(shard.cache)
		shard.mu.RUnlock()
	}

	capacity = pc.maxPerShard * 256
	return
}

// Clear empties the cache.
func (pc *PositionCache) Clear() {
	for _, shard := range pc.shards {
		shard.mu.Lock()
		shard.cache = make(map[[KeySize]byte]PositionRecord)
		shard.order = shard.order[:0]
		shard.mu.Unlock()
	}
	atomic.StoreUint64(&pc.hits, 0)
	atomic.StoreUint64(&pc.misses, 0)
}

// FileCache is a simple LRU cache for open position files
type FileCache struct {
	mu       sync.RWMutex
	cache    map[string]PositionFile
	order    []string
	maxFiles int
}

// NewFileCache creates a new file cache
func NewFileCache(maxFiles int) *FileCache {
	return &FileCache{
		cache:    make(map[string]PositionFile),
		order:    make([]string, 0, maxFiles),
		maxFiles: maxFiles,
	}
}

// Get retrieves a file from the cache
func (fc *FileCache) Get(path string) (PositionFile, bool) {
	fc.mu.RLock()
	f, ok := fc.cache[path]
	fc.mu.RUnlock()
	return f, ok
}

// Put adds a file to the cache, evicting oldest if necessary
func (fc *FileCache) Put(path string, f PositionFile) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	// Check if already exists
	if _, exists := fc.cache[path]; exists {
		fc.cache[path] = f
		return
	}

	// Skip caching if disabled
	if fc.maxFiles == 0 {
		return
	}

	// Evict oldest entries if cache is full
	for len(fc.cache) >= fc.maxFiles && len(fc.order) > 0 {
		oldest := fc.order[0]
		fc.order = fc.order[1:]
		delete(fc.cache, oldest)
	}

	fc.cache[path] = f
	fc.order = append(fc.order, path)
}

// Clear removes all files from the cache
func (fc *FileCache) Clear() {
	fc.mu.Lock()
	defer fc.mu.Unlock()
	fc.cache = make(map[string]PositionFile)
	fc.order = fc.order[:0]
}

// Size returns the number of cached files
func (fc *FileCache) Size() int {
	fc.mu.RLock()
	defer fc.mu.RUnlock()
	return len(fc.cache)
}

// L0FileIndex holds bloom filter and key range for fast L0 filtering
type L0FileIndex struct {
	MinKey [KeySize]byte
	MaxKey [KeySize]byte
	Bloom  []uint64 // bit array for bloom filter
	K      int      // number of hash functions
}

// bloomSize returns the number of uint64s needed for n keys with ~1% false positive rate
func bloomSize(n int) int {
	// ~10 bits per key for 1% FP rate, round up to uint64 boundary
	bits := n * 10
	return (bits + 63) / 64
}

// NewL0FileIndex creates an index from sorted records
func NewL0FileIndex(records []Record) *L0FileIndex {
	if len(records) == 0 {
		return nil
	}

	idx := &L0FileIndex{
		MinKey: records[0].Key,
		MaxKey: records[len(records)-1].Key,
		K:      7, // optimal k for 10 bits per element
	}

	// Create bloom filter
	size := bloomSize(len(records))
	idx.Bloom = make([]uint64, size)
	numBits := uint64(size * 64)

	for _, rec := range records {
		// Use FNV-1a hash with different seeds for k hash functions
		for i := 0; i < idx.K; i++ {
			h := fnvHash(rec.Key[:], uint64(i))
			bit := h % numBits
			idx.Bloom[bit/64] |= 1 << (bit % 64)
		}
	}

	return idx
}

// MayContain returns true if the key might be in this file (false = definitely not)
func (idx *L0FileIndex) MayContain(key [KeySize]byte) bool {
	// Quick range check first
	if compareKeys(key, idx.MinKey) < 0 || compareKeys(key, idx.MaxKey) > 0 {
		return false
	}

	// Bloom filter check
	numBits := uint64(len(idx.Bloom) * 64)
	for i := 0; i < idx.K; i++ {
		h := fnvHash(key[:], uint64(i))
		bit := h % numBits
		if idx.Bloom[bit/64]&(1<<(bit%64)) == 0 {
			return false
		}
	}
	return true
}

// fnvHash computes FNV-1a hash with a seed
func fnvHash(data []byte, seed uint64) uint64 {
	const prime = 1099511628211
	h := uint64(14695981039346656037) ^ seed
	for _, b := range data {
		h ^= uint64(b)
		h *= prime
	}
	return h
}

// compareKeys compares two keys, returning -1, 0, or 1
func compareKeys(a, b [KeySize]byte) int {
	for i := 0; i < KeySize; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return 0
}

// L0IndexCache is a cache of bloom filter indexes for L0 files
type L0IndexCache struct {
	mu    sync.RWMutex
	index map[string]*L0FileIndex
}

// NewL0IndexCache creates a new L0 index cache
func NewL0IndexCache() *L0IndexCache {
	return &L0IndexCache{
		index: make(map[string]*L0FileIndex),
	}
}

// Get retrieves an index from the cache
func (c *L0IndexCache) Get(path string) *L0FileIndex {
	c.mu.RLock()
	idx := c.index[path]
	c.mu.RUnlock()
	return idx
}

// Put adds an index to the cache
func (c *L0IndexCache) Put(path string, idx *L0FileIndex) {
	c.mu.Lock()
	c.index[path] = idx
	c.mu.Unlock()
}

// Clear removes all indexes from the cache
func (c *L0IndexCache) Clear() {
	c.mu.Lock()
	c.index = make(map[string]*L0FileIndex)
	c.mu.Unlock()
}
