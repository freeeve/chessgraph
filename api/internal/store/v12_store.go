package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/freeeve/chessgraph/api/internal/graph"
	"github.com/klauspost/compress/zstd"
)

// logMemStats logs current memory usage
func (s *V12Store) logMemStats(label string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Count cache sizes
	s.l0CacheMu.RLock()
	l0CacheFiles := len(s.l0Cache)
	s.l0CacheMu.RUnlock()

	s.l1CacheMu.RLock()
	l1CacheFiles := len(s.l1Cache)
	s.l1CacheMu.RUnlock()

	// Memtable size
	var memtableBytes int64
	for _, mt := range s.memtables {
		memtableBytes += mt.Size()
	}

	s.log("MEMORY [%s]: heap=%.2fGB, sys=%.2fGB, l0Cache=%d files, l1Cache=%d files, memtables=%.2fGB, goroutines=%d",
		label,
		float64(m.HeapAlloc)/(1024*1024*1024),
		float64(m.Sys)/(1024*1024*1024),
		l0CacheFiles,
		l1CacheFiles,
		float64(memtableBytes)/(1024*1024*1024),
		runtime.NumGoroutine())
}

// V12Metadata holds persistent store metadata
type V12Metadata struct {
	TotalGames  uint64 `json:"total_games"`
	TotalReads  uint64 `json:"total_reads"`
	TotalWrites uint64 `json:"total_writes"`
	L1FileCount int64  `json:"l1_file_count"`
}

// V12Store is the main store using V12 format
type V12Store struct {
	dir     string
	l0Dir   string // directory for L0 (unsorted) files
	l1Dir   string // directory for L1 (sorted) files
	index   *V12Index
	l0Encoder *zstd.Encoder // fast compression for L0
	l1Encoder *zstd.Encoder // best compression for L1
	decoder   *zstd.Decoder

	// Ingest state
	memtables       []*V12Memtable // one per worker
	memtableMu      sync.Mutex
	memtableFlushMu []sync.Mutex   // per-memtable flush locks (avoid global contention)
	l0FileCount     int64
	l1FileCount     int64        // counter for L1 file naming
	flushMu         sync.RWMutex // RLock for flushes, Lock for compaction
	l0ReserveMu     sync.Mutex   // serializes L0 slot reservation (backpressure check)
	pendingFlushes  int32        // atomic counter for in-flight L0 writes (for backpressure)
	compacting      int32        // atomic flag: 1 if compaction in progress
	l0Threshold     int          // trigger compaction when L0 files exceed this
	l0MaxFiles      int          // pause ingestion when L0 files exceed this

	// L0 file cache (avoid re-decompressing on every Get)
	l0Cache      map[string]*V12File
	l0CacheOrder []string // tracks insertion order for LRU eviction
	l0CacheMu    sync.RWMutex
	l0CacheMax   int // max number of cached files (0 = unlimited)

	// L1 file cache (avoid re-decompressing on every Get)
	l1Cache      map[string]*V12File
	l1CacheOrder []string // tracks insertion order for LRU eviction
	l1CacheMu    sync.RWMutex
	l1CacheMax   int // max number of cached files

	// Stats
	totalWrites uint64
	totalReads  uint64
	totalGames  uint64

	// Config
	targetL0Size int64 // target uncompressed L0 file size
	numWorkers   int

	logFunc func(format string, args ...any)
}

// IngestStore is the interface needed by the ingest worker
type IngestStore interface {
	Increment(key graph.PositionKey, wins, draws, losses uint16) error
	IncrementWorker(workerID int, key graph.PositionKey, wins, draws, losses uint16)
	FlushAll() error
	FlushIfMemoryNeeded(thresholdBytes int64) error
	FlushWorkerIfNeeded(workerID int) error // Flush specific worker's memtable if needed
	IncrementGameCount(n uint64)
	SetLogger(f func(format string, args ...any))
	NumWorkers() int
	Compact() error // Compact L0 -> L1 (merge and sort)
}

// V12StoreConfig configures the V12 store
type V12StoreConfig struct {
	Dir          string
	TargetL0Size int64 // target uncompressed L0 file size, default 512MB
	NumWorkers   int   // default runtime.NumCPU()
	L0Threshold  int   // trigger compaction when L0 files exceed this, default 5
	L0CacheMax   int   // max L0 files to cache in memory, default 50 (0 = unlimited)
	L0MaxFiles   int   // pause ingestion when L0 exceeds this, default 10
}

// NewV12Store creates a new V12 store
func NewV12Store(cfg V12StoreConfig) (*V12Store, error) {
	if cfg.TargetL0Size == 0 {
		cfg.TargetL0Size = 256 * 1024 * 1024 // 256MB default uncompressed L0 file size (smaller = less memory)
	}
	if cfg.NumWorkers == 0 {
		cfg.NumWorkers = runtime.NumCPU()
	}
	if cfg.L0Threshold == 0 {
		cfg.L0Threshold = 5 // trigger compaction when L0 exceeds 5 files
	}
	if cfg.L0CacheMax == 0 {
		cfg.L0CacheMax = 10 // cache up to 10 L0 files (each ~128MB uncompressed)
	}
	if cfg.L0MaxFiles == 0 {
		cfg.L0MaxFiles = 20 // pause ingestion when L0 exceeds 20 files
	}

	l0Dir := filepath.Join(cfg.Dir, "l0")
	l1Dir := filepath.Join(cfg.Dir, "l1")

	// Create directories
	if err := os.MkdirAll(l0Dir, 0755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(l1Dir, 0755); err != nil {
		return nil, err
	}

	// Fast encoder for L0 (prioritize speed during ingest)
	l0Encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}

	// Best encoder for L1 (prioritize compression for final storage)
	l1Encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return nil, err
	}

	// Use GOMAXPROCS for decoder concurrency (default is min(4, GOMAXPROCS) which limits parallel decompression)
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		return nil, err
	}

	// Create memtables for workers
	memtables := make([]*V12Memtable, cfg.NumWorkers)
	for i := range memtables {
		memtables[i] = NewV12Memtable()
	}

	s := &V12Store{
		dir:             cfg.Dir,
		l0Dir:           l0Dir,
		l1Dir:           l1Dir,
		index:           NewV12Index(),
		l0Encoder:       l0Encoder,
		l1Encoder:       l1Encoder,
		decoder:         decoder,
		memtables:       memtables,
		memtableFlushMu: make([]sync.Mutex, cfg.NumWorkers),
		targetL0Size:    cfg.TargetL0Size,
		numWorkers:      cfg.NumWorkers,
		l0Threshold:     cfg.L0Threshold,
		l0MaxFiles:      cfg.L0MaxFiles,
		l0Cache:         make(map[string]*V12File),
		l0CacheOrder:    make([]string, 0),
		l0CacheMax:      cfg.L0CacheMax,
		l1Cache:         make(map[string]*V12File),
		l1CacheOrder:    make([]string, 0),
		l1CacheMax:      4, // limit cached L1 files (each ~128MB uncompressed)
	}

	// Load existing L1 index
	if err := s.index.LoadFromDir(l1Dir, decoder); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load index: %w", err)
	}

	// Load metadata (games count, etc.)
	if err := s.loadMetadata(); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	// Initialize L1 file counter - use max of metadata value and actual file count
	// to handle cases where files were added but metadata wasn't saved
	if int64(s.index.FileCount()) > s.l1FileCount {
		s.l1FileCount = int64(s.index.FileCount())
	}

	return s, nil
}

// metadataPath returns the path to the metadata file
func (s *V12Store) metadataPath() string {
	return filepath.Join(s.dir, "metadata.json")
}

// loadMetadata loads persistent metadata from disk
func (s *V12Store) loadMetadata() error {
	data, err := os.ReadFile(s.metadataPath())
	if err != nil {
		return err
	}

	var meta V12Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return err
	}

	atomic.StoreUint64(&s.totalGames, meta.TotalGames)
	atomic.StoreUint64(&s.totalReads, meta.TotalReads)
	atomic.StoreUint64(&s.totalWrites, meta.TotalWrites)
	s.l1FileCount = meta.L1FileCount

	return nil
}

// saveMetadata saves persistent metadata to disk
func (s *V12Store) saveMetadata() error {
	meta := V12Metadata{
		TotalGames:  atomic.LoadUint64(&s.totalGames),
		TotalReads:  atomic.LoadUint64(&s.totalReads),
		TotalWrites: atomic.LoadUint64(&s.totalWrites),
		L1FileCount: atomic.LoadInt64(&s.l1FileCount),
	}

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}

	// Write to temp file then rename for atomicity
	tempPath := s.metadataPath() + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tempPath, s.metadataPath())
}

// SetLogger sets the logging function
func (s *V12Store) SetLogger(f func(format string, args ...any)) {
	s.logFunc = f
}

func (s *V12Store) log(format string, args ...any) {
	if s.logFunc != nil {
		s.logFunc(format, args...)
	}
}

// Get retrieves a record by position key, merging data from all layers
func (s *V12Store) Get(key graph.PositionKey) (*PositionRecord, error) {
	atomic.AddUint64(&s.totalReads, 1)

	var k [V12KeySize]byte
	copy(k[:], key[:])

	var result *PositionRecord

	// Check all memtables and merge
	for _, mt := range s.memtables {
		if rec := mt.Get(k); rec != nil {
			if result == nil {
				result = &PositionRecord{}
				*result = *rec
			} else {
				mergeRecords(result, rec)
			}
		}
	}

	// Check L0 files and merge
	l0Recs, _ := s.getFromL0(k)
	for _, rec := range l0Recs {
		if result == nil {
			result = &PositionRecord{}
			*result = rec
		} else {
			mergeRecords(result, &rec)
		}
	}

	// Check L1 and merge
	fileIdx := s.index.FindFile(k)
	if fileIdx >= 0 {
		files := s.index.Files()
		f, err := s.getL1Cached(files[fileIdx].Path)
		if err == nil {
			if rec, err := f.Get(k); err == nil && rec != nil {
				if result == nil {
					result = rec
				} else {
					mergeRecords(result, rec)
				}
			}
		}
	}

	if result == nil {
		return nil, ErrNotFound
	}
	return result, nil
}

// getFromL0 retrieves all records for a key from L0 files (uses cache)
func (s *V12Store) getFromL0(key [V12KeySize]byte) ([]PositionRecord, error) {
	entries, err := os.ReadDir(s.l0Dir)
	if err != nil {
		return nil, err
	}

	var results []PositionRecord
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".psv2" {
			continue
		}
		path := filepath.Join(s.l0Dir, e.Name())
		f := s.getL0Cached(path)
		if f == nil {
			continue
		}
		if rec, err := f.Get(key); err == nil && rec != nil {
			results = append(results, *rec)
		}
	}
	return results, nil
}

// getL0Cached returns a cached L0 file, loading it if necessary
func (s *V12Store) getL0Cached(path string) *V12File {
	// Check cache first
	s.l0CacheMu.RLock()
	f, ok := s.l0Cache[path]
	s.l0CacheMu.RUnlock()
	if ok {
		return f
	}

	// Load and cache
	s.l0CacheMu.Lock()
	defer s.l0CacheMu.Unlock()

	// Double-check after acquiring write lock
	if f, ok := s.l0Cache[path]; ok {
		return f
	}

	f, err := OpenV12File(path, s.decoder)
	if err != nil {
		return nil
	}

	// Evict oldest entries if cache is full
	for s.l0CacheMax > 0 && len(s.l0Cache) >= s.l0CacheMax && len(s.l0CacheOrder) > 0 {
		oldest := s.l0CacheOrder[0]
		s.l0CacheOrder = s.l0CacheOrder[1:]
		delete(s.l0Cache, oldest)
	}

	s.l0Cache[path] = f
	s.l0CacheOrder = append(s.l0CacheOrder, path)
	s.log("cached L0 file: %s (%d records)", filepath.Base(path), f.RecordCount())
	return f
}

// clearL0Cache clears the L0 file cache (called after compaction)
func (s *V12Store) clearL0Cache() {
	s.l0CacheMu.Lock()
	defer s.l0CacheMu.Unlock()
	s.l0Cache = make(map[string]*V12File)
	s.l0CacheOrder = s.l0CacheOrder[:0]
}

// getL1Cached returns an L1 file from cache or loads it
func (s *V12Store) getL1Cached(path string) (*V12File, error) {
	s.l1CacheMu.RLock()
	if f, ok := s.l1Cache[path]; ok {
		s.l1CacheMu.RUnlock()
		return f, nil
	}
	s.l1CacheMu.RUnlock()

	// Load file
	s.l1CacheMu.Lock()
	defer s.l1CacheMu.Unlock()

	// Double-check after acquiring write lock
	if f, ok := s.l1Cache[path]; ok {
		return f, nil
	}

	f, err := OpenV12File(path, s.decoder)
	if err != nil {
		return nil, err
	}

	// Evict oldest entries if cache is full
	for s.l1CacheMax > 0 && len(s.l1Cache) >= s.l1CacheMax && len(s.l1CacheOrder) > 0 {
		oldest := s.l1CacheOrder[0]
		s.l1CacheOrder = s.l1CacheOrder[1:]
		delete(s.l1Cache, oldest)
	}

	s.l1Cache[path] = f
	s.l1CacheOrder = append(s.l1CacheOrder, path)
	return f, nil
}

// clearL1Cache clears the L1 file cache (called after compaction)
func (s *V12Store) clearL1Cache() {
	s.l1CacheMu.Lock()
	defer s.l1CacheMu.Unlock()
	s.l1Cache = make(map[string]*V12File)
	s.l1CacheOrder = s.l1CacheOrder[:0]
}

// evictL1CacheEntry removes a specific path from L1 cache
func (s *V12Store) evictL1CacheEntry(path string) {
	s.l1CacheMu.Lock()
	defer s.l1CacheMu.Unlock()
	delete(s.l1Cache, path)
	// Rebuild cache order without this path
	newOrder := make([]string, 0, len(s.l1CacheOrder))
	for _, p := range s.l1CacheOrder {
		if p != path {
			newOrder = append(newOrder, p)
		}
	}
	s.l1CacheOrder = newOrder
}

// mergeRecords merges src into dst (sum W/D/L, keep eval if present)
func mergeRecords(dst, src *PositionRecord) {
	dst.Wins = saturatingAdd16(dst.Wins, src.Wins)
	dst.Draws = saturatingAdd16(dst.Draws, src.Draws)
	dst.Losses = saturatingAdd16(dst.Losses, src.Losses)
	// Keep eval from src if it has one and dst doesn't
	if src.HasCP() && !dst.HasCP() {
		dst.CP = src.CP
		dst.DTM = src.DTM
		dst.DTZ = src.DTZ
		dst.ProvenDepth = src.ProvenDepth
		dst.SetHasCP(true)
	}
}

// Put writes a record (for eval updates, etc.)
func (s *V12Store) Put(key graph.PositionKey, rec *PositionRecord) error {
	atomic.AddUint64(&s.totalWrites, 1)

	var k [V12KeySize]byte
	copy(k[:], key[:])

	// Write to first memtable (for non-parallel updates)
	s.memtables[0].Put(k, rec)

	// Check if flush needed
	if s.memtables[0].FileSize() >= s.targetL0Size {
		return s.FlushMemtable(0)
	}
	return nil
}

// Increment adds W/D/L to a position (implements IngestStore)
func (s *V12Store) Increment(key graph.PositionKey, wins, draws, losses uint16) error {
	atomic.AddUint64(&s.totalWrites, 1)

	var k [V12KeySize]byte
	copy(k[:], key[:])

	// Use first memtable for single-threaded ingest
	s.memtables[0].Increment(k, wins, draws, losses)
	return nil
}

// IncrementWorker adds W/D/L to a position for a specific worker (for parallel ingest)
func (s *V12Store) IncrementWorker(workerID int, key graph.PositionKey, wins, draws, losses uint16) {
	atomic.AddUint64(&s.totalWrites, 1)

	var k [V12KeySize]byte
	copy(k[:], key[:])

	idx := workerID % len(s.memtables)
	s.memtables[idx].Increment(k, wins, draws, losses)
}

// IncrementGameCount adds to the total game count (implements IngestStore)
func (s *V12Store) IncrementGameCount(n uint64) {
	atomic.AddUint64(&s.totalGames, n)
}

// FlushIfMemoryNeeded flushes individual memtables that exceed target L0 file size.
// Prefer FlushWorkerIfNeeded for parallel ingest to avoid lock contention.
func (s *V12Store) FlushIfMemoryNeeded(thresholdBytes int64) error {
	// Check each memtable and flush those exceeding target file size
	for i, mt := range s.memtables {
		if mt.FileSize() >= s.targetL0Size {
			if err := s.FlushMemtable(i); err != nil {
				return err
			}
		}
	}
	return nil
}

// FlushWorkerIfNeeded flushes a specific worker's memtable if it exceeds target size.
// This avoids lock contention by only checking the worker's own memtable.
func (s *V12Store) FlushWorkerIfNeeded(workerID int) error {
	idx := workerID % len(s.memtables)
	mt := s.memtables[idx]
	if mt.FileSize() >= s.targetL0Size {
		return s.FlushMemtable(idx)
	}
	return nil
}

// CheckFlush checks if any memtable needs flushing and flushes if so
func (s *V12Store) CheckFlush() error {
	for i, mt := range s.memtables {
		if mt.FileSize() >= s.targetL0Size {
			if err := s.FlushMemtable(i); err != nil {
				return err
			}
		}
	}
	return nil
}

// FlushMemtable flushes a specific memtable to L0
func (s *V12Store) FlushMemtable(idx int) error {
	// Per-memtable lock allows concurrent flushes from different workers
	s.memtableFlushMu[idx].Lock()
	defer s.memtableFlushMu[idx].Unlock()

	// Wait for L0 space and reserve our slot (increments pendingFlushes)
	s.waitForL0SpaceAndReserve()
	// Release our slot after flush completes (or fails)
	defer atomic.AddInt32(&s.pendingFlushes, -1)

	s.flushMu.RLock() // Block during compaction
	defer s.flushMu.RUnlock()
	return s.flushMemtableInner(idx)
}

// flushMemtableInner flushes a memtable (caller must hold flushMu read or write lock)
func (s *V12Store) flushMemtableInner(idx int) error {
	mt := s.memtables[idx]
	records := mt.Flush()
	if len(records) == 0 {
		return nil
	}

	fileNum := atomic.AddInt64(&s.l0FileCount, 1)
	filename := fmt.Sprintf("l0_%06d.psv2", fileNum)
	path := filepath.Join(s.l0Dir, filename)

	if err := WriteV12File(path, records, s.l0Encoder); err != nil {
		return fmt.Errorf("write L0 file: %w", err)
	}

	// Log with compression stats
	uncompressedSize := int64(len(records)) * V12RecordSize
	if fi, err := os.Stat(path); err == nil {
		compressedSize := fi.Size()
		ratio := float64(uncompressedSize) / float64(compressedSize)
		s.log("flushed memtable %d: %d records -> %s (%.1fMB -> %.1fMB, %.1fx, compressed in %v)",
			idx, len(records), filename,
			float64(uncompressedSize)/(1024*1024),
			float64(compressedSize)/(1024*1024),
			ratio,
			LastCompressTime)
	} else {
		s.log("flushed memtable %d: %d records -> %s", idx, len(records), filename)
	}

	// Check if we should trigger background compaction
	s.maybeCompactAsync()

	return nil
}

// maybeCompactAsync triggers background compaction if L0 file count exceeds threshold
func (s *V12Store) maybeCompactAsync() {
	// Count L0 files
	entries, err := os.ReadDir(s.l0Dir)
	if err != nil {
		return
	}
	l0Count := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".psv2" {
			l0Count++
		}
	}

	// Trigger compaction if over threshold (CompactL0 handles its own concurrency check)
	if l0Count > s.l0Threshold {
		s.log("L0 files (%d) exceeded threshold (%d), starting background compaction", l0Count, s.l0Threshold)
		go func() {
			if err := s.CompactL0(); err != nil {
				s.log("background compaction failed: %v", err)
			}
		}()
	}
}

// waitForL0SpaceAndReserve waits until there's room for another L0 file,
// then atomically reserves a slot by incrementing pendingFlushes.
// Caller must call atomic.AddInt32(&s.pendingFlushes, -1) after writing.
func (s *V12Store) waitForL0SpaceAndReserve() {
	logged := false
	for {
		// Serialize the check+reserve to prevent race where multiple workers
		// all see room and all reserve before any writes complete
		s.l0ReserveMu.Lock()

		entries, err := os.ReadDir(s.l0Dir)
		if err != nil {
			atomic.AddInt32(&s.pendingFlushes, 1)
			s.l0ReserveMu.Unlock()
			return
		}
		l0Count := 0
		for _, e := range entries {
			if !e.IsDir() && filepath.Ext(e.Name()) == ".psv2" {
				l0Count++
			}
		}

		// Include pending flushes (in-flight writes) in the count
		pending := atomic.LoadInt32(&s.pendingFlushes)
		total := l0Count + int(pending)

		if total < s.l0MaxFiles {
			// Reserve our slot while holding the lock
			atomic.AddInt32(&s.pendingFlushes, 1)
			s.l0ReserveMu.Unlock()
			return
		}

		s.l0ReserveMu.Unlock()

		if !logged {
			s.log("L0 files (%d on disk + %d pending = %d) at max (%d), waiting for compaction...",
				l0Count, pending, total, s.l0MaxFiles)
			logged = true
			// Ensure compaction is running
			go s.CompactL0()
		}

		// Wait before checking again
		time.Sleep(5 * time.Second)
	}
}

// FlushAll flushes all memtables (respects backpressure)
func (s *V12Store) FlushAll() error {
	for i := range s.memtables {
		if err := s.FlushMemtable(i); err != nil {
			return err
		}
	}
	return nil
}

// Compact merges L0 files into L1 (implements IngestStore)
func (s *V12Store) Compact() error {
	return s.CompactL0()
}

// CompactL0 merges L0 files into L1 incrementally (a few L0 files at a time)
func (s *V12Store) CompactL0() error {
	// Check if already compacting - only one compaction can run at a time
	if !atomic.CompareAndSwapInt32(&s.compacting, 0, 1) {
		s.log("compaction already in progress, skipping")
		return nil
	}
	defer atomic.StoreInt32(&s.compacting, 0)

	s.logMemStats("compaction-start")

	const l0BatchSize = 10 // Process this many L0 files at a time

	for {
		// Snapshot L0 files
		entries, err := os.ReadDir(s.l0Dir)
		if err != nil {
			return err
		}

		var l0Files []string
		for _, e := range entries {
			if !e.IsDir() && filepath.Ext(e.Name()) == ".psv2" {
				l0Files = append(l0Files, filepath.Join(s.l0Dir, e.Name()))
			}
		}

		if len(l0Files) == 0 {
			s.log("compaction complete: all L0 files processed")
			// Save metadata to persist L1 file count and stats
			if err := s.saveMetadata(); err != nil {
				s.log("warning: failed to save metadata: %v", err)
			}
			return nil
		}

		// Take a batch
		batch := l0Files
		if len(batch) > l0BatchSize {
			batch = batch[:l0BatchSize]
		}

		s.log("compacting batch of %d L0 files (%d total remaining)...", len(batch), len(l0Files))

		// Merge this batch into L1
		if err := s.mergeL0BatchIntoL1(batch); err != nil {
			s.log("compaction failed: %v", err)
			return fmt.Errorf("merge L0 batch: %w", err)
		}

		// Delete processed L0 files
		for _, f := range batch {
			os.Remove(f)
		}

		// Clear L0 cache for deleted files
		s.l0CacheMu.Lock()
		for _, f := range batch {
			delete(s.l0Cache, f)
		}
		// Rebuild cache order without deleted files
		newOrder := make([]string, 0, len(s.l0CacheOrder))
		for _, path := range s.l0CacheOrder {
			if _, exists := s.l0Cache[path]; exists {
				newOrder = append(newOrder, path)
			}
		}
		s.l0CacheOrder = newOrder
		s.l0CacheMu.Unlock()

		s.log("batch complete: %d L1 files, %d total records",
			s.index.FileCount(), s.index.TotalRecords())

		// Save metadata after each batch to persist progress
		if err := s.saveMetadata(); err != nil {
			s.log("warning: failed to save metadata: %v", err)
		}

		// Force GC to release compaction memory
		runtime.GC()
		s.logMemStats("after-batch-gc")

		// Continue loop - it will re-read L0 dir and exit when empty
	}
}

// mergeL0BatchIntoL1 merges a batch of L0 files into L1
func (s *V12Store) mergeL0BatchIntoL1(l0Paths []string) error {
	if len(l0Paths) == 0 {
		return nil
	}

	// Load L0 files in parallel
	type l0Result struct {
		idx     int
		records []V12Record
		err     error
	}
	l0ResultChan := make(chan l0Result, len(l0Paths))

	for i, path := range l0Paths {
		go func(idx int, p string) {
			f, err := OpenV12File(p, s.decoder)
			if err != nil {
				l0ResultChan <- l0Result{idx: idx, err: fmt.Errorf("open L0 %s: %w", p, err)}
				return
			}
			// Pre-allocate to avoid repeated slice growth
			records := make([]V12Record, 0, f.RecordCount())
			iter := f.Iterator()
			for {
				rec := iter.Next()
				if rec == nil {
					break
				}
				records = append(records, *rec)
			}
			l0ResultChan <- l0Result{idx: idx, records: records}
		}(i, path)
	}

	// Collect L0 results
	l0Results := make([]l0Result, len(l0Paths))
	for range l0Paths {
		r := <-l0ResultChan
		l0Results[r.idx] = r
	}

	// Check for errors and count total records for pre-allocation
	totalRecords := 0
	for _, r := range l0Results {
		if r.err != nil {
			return r.err
		}
		totalRecords += len(r.records)
	}

	// Merge records with pre-allocated capacity
	allL0Records := make([]V12Record, 0, totalRecords)
	for _, r := range l0Results {
		allL0Records = append(allL0Records, r.records...)
	}

	s.logMemStats(fmt.Sprintf("after-l0-load (%d records)", len(allL0Records)))

	if len(allL0Records) == 0 {
		return nil
	}

	// Sort L0 records by key using parallel sort for large sets
	sortStart := time.Now()
	allL0Records = parallelSortRecords(allL0Records, runtime.NumCPU())
	s.log("  sorted %d L0 records in %v using %d workers", len(allL0Records), time.Since(sortStart), runtime.NumCPU())

	s.logMemStats("after-sort")

	// Merge duplicate keys within L0
	allL0Records = mergeAdjacentRecords(allL0Records)

	s.log("  L0 batch: %d records after merge", len(allL0Records))
	s.logMemStats("after-merge-adjacent")

	// Get L1 files sorted by MinKey
	l1Files := s.index.Files()
	sort.Slice(l1Files, func(i, j int) bool {
		return bytes.Compare(l1Files[i].MinKey[:], l1Files[j].MinKey[:]) < 0
	})

	// If no L1 files yet, write L0 directly as new L1
	if len(l1Files) == 0 {
		return s.writeNewL1Files(allL0Records)
	}

	// Find L0 key range
	l0MinKey := allL0Records[0].Key
	l0MaxKey := allL0Records[len(allL0Records)-1].Key

	// Find affected L1 files (those that overlap with L0 key range)
	// Also include first/last L1 files if L0 has records outside current L1 range
	var affectedFiles []V12FileRange
	var mergeBeforeIntoFirst, mergeAfterIntoLast bool

	// Check if L0 has records before first L1 file - if so, include first L1 in affected
	if len(l1Files) > 0 && bytes.Compare(l0MinKey[:], l1Files[0].MinKey[:]) < 0 {
		mergeBeforeIntoFirst = true
	}

	// Check if L0 has records after last L1 file - if so, include last L1 in affected
	if len(l1Files) > 0 && bytes.Compare(l0MaxKey[:], l1Files[len(l1Files)-1].MaxKey[:]) > 0 {
		mergeAfterIntoLast = true
	}

	for i, f := range l1Files {
		// Check if L1 file overlaps with L0 range
		overlaps := bytes.Compare(f.MaxKey[:], l0MinKey[:]) >= 0 && bytes.Compare(f.MinKey[:], l0MaxKey[:]) <= 0
		// Also include first file if we need to merge "before" records into it
		includeFirst := mergeBeforeIntoFirst && i == 0
		// Also include last file if we need to merge "after" records into it
		includeLast := mergeAfterIntoLast && i == len(l1Files)-1

		if overlaps || includeFirst || includeLast {
			affectedFiles = append(affectedFiles, f)
		}
	}

	s.log("  affected L1 files: %d (merge-before: %v, merge-after: %v)",
		len(affectedFiles), mergeBeforeIntoFirst, mergeAfterIntoLast)

	// Pre-compute L0 record ranges for each affected L1 file
	type l1Work struct {
		l1Range   V12FileRange
		l0Start   int
		l0End     int
		isFirst   bool
		isLast    bool
	}
	workItems := make([]l1Work, len(affectedFiles))
	l0Idx := 0

	for i, l1Range := range affectedFiles {
		isFirst := i == 0
		isLast := i == len(affectedFiles)-1

		l0Start := l0Idx
		for l0Idx < len(allL0Records) {
			rec := allL0Records[l0Idx]

			belongsHere := false
			if isFirst && isLast {
				belongsHere = true
			} else if isFirst {
				belongsHere = bytes.Compare(rec.Key[:], l1Range.MaxKey[:]) <= 0
			} else if isLast {
				belongsHere = true
			} else {
				belongsHere = bytes.Compare(rec.Key[:], l1Range.MaxKey[:]) <= 0
			}

			if !belongsHere {
				break
			}
			l0Idx++
		}

		workItems[i] = l1Work{
			l1Range: l1Range,
			l0Start: l0Start,
			l0End:   l0Idx,
			isFirst: isFirst,
			isLast:  isLast,
		}
	}

	// Process L1 files in parallel (limited concurrency to control memory)
	type l1Result struct {
		idx   int
		files []V12FileRange
		err   error
	}

	const maxConcurrentL1 = 16 // Limit concurrent L1 files in memory (~2GB max)
	semaphore := make(chan struct{}, maxConcurrentL1)

	resultChan := make(chan l1Result, len(workItems))
	for i, work := range workItems {
		go func(idx int, w l1Work) {
			// Acquire semaphore slot
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Load L1 file
			l1File, err := OpenV12File(w.l1Range.Path, s.decoder)
			if err != nil {
				resultChan <- l1Result{idx: idx, err: fmt.Errorf("open L1 %s: %w", w.l1Range.Path, err)}
				return
			}

			// Get L1 records (pre-allocate to avoid slice growth garbage)
			l1Records := make([]V12Record, 0, l1File.RecordCount())
			iter := l1File.Iterator()
			for {
				rec := iter.Next()
				if rec == nil {
					break
				}
				l1Records = append(l1Records, *rec)
			}

			// Get L0 records for this file
			l0ForThisFile := allL0Records[w.l0Start:w.l0End]

			// Merge L1 + L0 records
			merged := MergeV12Records(l1Records, l0ForThisFile)

			// Write merged file (may split if too large)
			written, err := s.writeMergedL1File(w.l1Range.Path, merged)
			if err != nil {
				resultChan <- l1Result{idx: idx, err: err}
				return
			}

			resultChan <- l1Result{idx: idx, files: written}
		}(i, work)
	}

	// Collect results in order
	results := make([]l1Result, len(workItems))
	for range workItems {
		r := <-resultChan
		results[r.idx] = r
	}

	s.logMemStats("after-l1-processing")

	// Check for errors and collect new files
	var newFiles []V12FileRange
	for _, r := range results {
		if r.err != nil {
			return r.err
		}
		newFiles = append(newFiles, r.files...)
	}

	// Track which paths we wrote to (so we don't delete them)
	newPaths := make(map[string]bool)
	for _, f := range newFiles {
		newPaths[f.Path] = true
	}

	// Update index: remove old affected files, add new ones
	s.flushMu.Lock()
	s.index.mu.Lock()

	// Remove affected files from index
	affectedPaths := make(map[string]bool)
	for _, f := range affectedFiles {
		affectedPaths[f.Path] = true
	}
	var remainingFiles []V12FileRange
	for _, f := range s.index.files {
		if !affectedPaths[f.Path] {
			remainingFiles = append(remainingFiles, f)
		}
	}

	// Add new files
	s.index.files = append(remainingFiles, newFiles...)

	// Re-sort by MinKey
	sort.Slice(s.index.files, func(i, j int) bool {
		return bytes.Compare(s.index.files[i].MinKey[:], s.index.files[j].MinKey[:]) < 0
	})

	s.index.mu.Unlock()
	s.flushMu.Unlock()

	// Evict all affected files from L1 cache (they've been modified or deleted)
	for _, f := range affectedFiles {
		s.evictL1CacheEntry(f.Path)
	}

	// Delete old L1 files that were replaced (but not overwritten in-place)
	for _, f := range affectedFiles {
		if !newPaths[f.Path] {
			os.Remove(f.Path)
		}
	}

	return nil
}

// mergeAdjacentRecords merges records with the same key (input must be sorted)
func mergeAdjacentRecords(records []V12Record) []V12Record {
	if len(records) == 0 {
		return records
	}
	result := make([]V12Record, 0, len(records))
	result = append(result, records[0])

	for i := 1; i < len(records); i++ {
		if bytes.Equal(records[i].Key[:], result[len(result)-1].Key[:]) {
			// Merge into last record
			last := &result[len(result)-1]
			last.Value.Wins = saturatingAdd16(last.Value.Wins, records[i].Value.Wins)
			last.Value.Draws = saturatingAdd16(last.Value.Draws, records[i].Value.Draws)
			last.Value.Losses = saturatingAdd16(last.Value.Losses, records[i].Value.Losses)
			if records[i].Value.HasCP() {
				last.Value.CP = records[i].Value.CP
				last.Value.DTM = records[i].Value.DTM
				last.Value.DTZ = records[i].Value.DTZ
				last.Value.ProvenDepth = records[i].Value.ProvenDepth
				last.Value.SetHasCP(true)
			}
		} else {
			result = append(result, records[i])
		}
	}
	return result
}

// writeNewL1Files writes records as new L1 files (when no L1 exists)
func (s *V12Store) writeNewL1Files(records []V12Record) error {
	written, err := s.writeNewL1FilesChunked(records)
	if err != nil {
		return err
	}

	s.flushMu.Lock()
	s.index.mu.Lock()
	s.index.files = append(s.index.files, written...)
	sort.Slice(s.index.files, func(i, j int) bool {
		return bytes.Compare(s.index.files[i].MinKey[:], s.index.files[j].MinKey[:]) < 0
	})
	s.index.mu.Unlock()
	s.flushMu.Unlock()

	return nil
}

// nextL1Filename generates the next L1 filename (l1_000001.psv2, etc.)
func (s *V12Store) nextL1Filename() string {
	n := atomic.AddInt64(&s.l1FileCount, 1)
	return fmt.Sprintf("l1_%06d.psv2", n)
}

// writeNewL1Files writes records as new L1 files (in parallel)
func (s *V12Store) writeNewL1FilesChunked(records []V12Record) ([]V12FileRange, error) {
	if len(records) == 0 {
		return nil, nil
	}

	// Pre-compute chunks
	targetRecords := int(V12MinFileSize / V12RecordSize)
	type chunkInfo struct {
		records []V12Record
		path    string
	}
	var chunks []chunkInfo

	for start := 0; start < len(records); {
		end := start + targetRecords
		if end > len(records) {
			end = len(records)
		}
		// Make sure last chunk isn't too small
		if len(records)-end < targetRecords/2 {
			end = len(records)
		}

		filename := s.nextL1Filename()
		path := filepath.Join(s.l1Dir, filename)
		chunks = append(chunks, chunkInfo{records: records[start:end], path: path})
		start = end
	}

	// Write chunks in parallel
	type writeResult struct {
		idx          int
		file         V12FileRange
		compressTime time.Duration
		err          error
	}
	resultChan := make(chan writeResult, len(chunks))

	for i, chunk := range chunks {
		go func(idx int, c chunkInfo) {
			stats, err := WriteV12FileWithStats(c.path, c.records, s.l1Encoder)
			if err != nil {
				resultChan <- writeResult{idx: idx, err: err}
				return
			}

			resultChan <- writeResult{
				idx:          idx,
				compressTime: stats.CompressTime,
				file: V12FileRange{
					Path:   c.path,
					MinKey: c.records[0].Key,
					MaxKey: c.records[len(c.records)-1].Key,
					Count:  uint32(len(c.records)),
				},
			}
		}(i, chunk)
	}

	// Collect results in order
	results := make([]writeResult, len(chunks))
	for range chunks {
		r := <-resultChan
		results[r.idx] = r
	}

	// Check for errors and build result
	var result []V12FileRange
	for i, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		result = append(result, r.file)

		// Log compression stats
		chunk := chunks[i]
		uncompressedSize := int64(len(chunk.records)) * V12RecordSize
		if fi, err := os.Stat(chunk.path); err == nil {
			compressedSize := fi.Size()
			ratio := float64(uncompressedSize) / float64(compressedSize)
			s.log("  wrote %s: %d records, keys %x..%x (%.1fMB -> %.1fMB, %.1fx, compressed in %v)",
				filepath.Base(chunk.path), len(chunk.records),
				chunk.records[0].Key[:], chunk.records[len(chunk.records)-1].Key[:],
				float64(uncompressedSize)/(1024*1024),
				float64(compressedSize)/(1024*1024),
				ratio,
				r.compressTime)
		}
	}

	return result, nil
}

// writeMergedL1File writes merged records, replacing an existing L1 file
func (s *V12Store) writeMergedL1File(oldPath string, records []V12Record) ([]V12FileRange, error) {
	if len(records) == 0 {
		return nil, nil
	}

	// Check if we need to split
	totalSize := int64(len(records)) * V12RecordSize
	if totalSize <= V12MaxFileSize {
		// Write in place (to temp, then rename)
		tempPath := oldPath + ".tmp"
		stats, err := WriteV12FileWithStats(tempPath, records, s.l1Encoder)
		if err != nil {
			return nil, err
		}
		if err := os.Rename(tempPath, oldPath); err != nil {
			os.Remove(tempPath)
			return nil, err
		}

		// Log compression stats
		uncompressedSize := int64(len(records)) * V12RecordSize
		if fi, err := os.Stat(oldPath); err == nil {
			compressedSize := fi.Size()
			ratio := float64(uncompressedSize) / float64(compressedSize)
			s.log("  wrote %s: %d records, keys %x..%x (%.1fMB -> %.1fMB, %.1fx, compressed in %v)",
				filepath.Base(oldPath), len(records),
				records[0].Key[:], records[len(records)-1].Key[:],
				float64(uncompressedSize)/(1024*1024),
				float64(compressedSize)/(1024*1024),
				ratio,
				stats.CompressTime)
		}

		return []V12FileRange{{
			Path:   oldPath,
			MinKey: records[0].Key,
			MaxKey: records[len(records)-1].Key,
			Count:  uint32(len(records)),
		}}, nil
	}

	// Need to split - write multiple files with fresh names
	var result []V12FileRange
	targetRecords := int(V12MinFileSize / V12RecordSize)
	first := true

	for start := 0; start < len(records); {
		end := start + targetRecords
		if end > len(records) {
			end = len(records)
		}
		if len(records)-end < targetRecords/2 {
			end = len(records)
		}

		chunk := records[start:end]
		var path string
		if first {
			// First chunk reuses original path (in-place update)
			path = oldPath
			first = false
		} else {
			// Additional chunks get new sequential names
			path = filepath.Join(s.l1Dir, s.nextL1Filename())
		}

		tempPath := path + ".tmp"
		stats, err := WriteV12FileWithStats(tempPath, chunk, s.l1Encoder)
		if err != nil {
			return nil, err
		}
		if err := os.Rename(tempPath, path); err != nil {
			os.Remove(tempPath)
			return nil, err
		}

		// Log compression stats
		uncompressedSize := int64(len(chunk)) * V12RecordSize
		if fi, err := os.Stat(path); err == nil {
			compressedSize := fi.Size()
			ratio := float64(uncompressedSize) / float64(compressedSize)
			s.log("  wrote %s: %d records, keys %x..%x (%.1fMB -> %.1fMB, %.1fx, compressed in %v)",
				filepath.Base(path), len(chunk),
				chunk[0].Key[:], chunk[len(chunk)-1].Key[:],
				float64(uncompressedSize)/(1024*1024),
				float64(compressedSize)/(1024*1024),
				ratio,
				stats.CompressTime)
		}

		result = append(result, V12FileRange{
			Path:   path,
			MinKey: chunk[0].Key,
			MaxKey: chunk[len(chunk)-1].Key,
			Count:  uint32(len(chunk)),
		})

		start = end
	}

	return result, nil
}

// Stats returns store statistics (implements ReadStore interface)
func (s *V12Store) Stats() PSStats {
	var memtableSize int64
	var memtableRecords int
	for _, mt := range s.memtables {
		memtableSize += mt.Size()
		memtableRecords += mt.Count()
	}

	// Count L0 files and size
	var l0FileCount int
	var l0CompressedBytes int64
	var l0Records uint64
	if entries, err := os.ReadDir(s.l0Dir); err == nil {
		for _, e := range entries {
			if !e.IsDir() && filepath.Ext(e.Name()) == ".psv2" {
				l0FileCount++
				path := filepath.Join(s.l0Dir, e.Name())
				if info, err := e.Info(); err == nil {
					l0CompressedBytes += info.Size()
				}
				// Read header to get record count (no decompression needed)
				if header, err := ReadV12Header(path); err == nil {
					l0Records += uint64(header.RecordCount)
				}
			}
		}
	}

	// L1 stats
	l1FileCount := s.index.FileCount()
	l1TotalRecords := s.index.TotalRecords()

	// Calculate L1 compressed size
	var l1CompressedBytes int64
	for _, f := range s.index.Files() {
		if info, err := os.Stat(f.Path); err == nil {
			l1CompressedBytes += info.Size()
		}
	}

	totalRecords := l1TotalRecords + l0Records + uint64(memtableRecords)
	totalFiles := l1FileCount + l0FileCount
	compressedBytes := uint64(l1CompressedBytes + l0CompressedBytes)
	uncompressedBytes := totalRecords * V12RecordSize

	s.l0CacheMu.RLock()
	cachedL0Files := len(s.l0Cache)
	s.l0CacheMu.RUnlock()

	return PSStats{
		TotalReads:        atomic.LoadUint64(&s.totalReads),
		TotalWrites:       atomic.LoadUint64(&s.totalWrites),
		DirtyFiles:        l0FileCount,
		DirtyBytes:        memtableSize,
		CachedBlocks:      cachedL0Files,
		TotalPositions:    totalRecords,
		TotalGames:        atomic.LoadUint64(&s.totalGames),
		TotalFolders:      uint64(l1FileCount),
		TotalBlocks:       uint64(totalFiles),
		UncompressedBytes: uncompressedBytes,
		CompressedBytes:   compressedBytes,
	}
}

// Close closes the store
func (s *V12Store) Close() error {
	// Flush remaining memtables
	if err := s.FlushAll(); err != nil {
		return err
	}
	// Save metadata
	if err := s.saveMetadata(); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}
	return nil
}

// MemtableSize returns total memtable memory usage
func (s *V12Store) MemtableSize() int64 {
	var total int64
	for _, mt := range s.memtables {
		total += mt.Size()
	}
	return total
}

// NumWorkers returns the number of ingest workers
func (s *V12Store) NumWorkers() int {
	return s.numWorkers
}

// IsReadOnly returns whether the store is in read-only mode (implements ReadStore interface)
func (s *V12Store) IsReadOnly() bool {
	return false
}

// IterateAll iterates over all records in the store (L1 files only, for export).
// The callback receives each key and record. Return false to stop iteration.
func (s *V12Store) IterateAll(fn func(key [V12KeySize]byte, rec *PositionRecord) bool) error {
	// Iterate L1 files
	for _, fileRange := range s.index.Files() {
		f, err := OpenV12File(fileRange.Path, s.decoder)
		if err != nil {
			return fmt.Errorf("open L1 file %s: %w", fileRange.Path, err)
		}

		iter := f.Iterator()
		for {
			rec := iter.Next()
			if rec == nil {
				break
			}
			if !fn(rec.Key, &rec.Value) {
				return nil
			}
		}
	}
	return nil
}

// FlushAllAsync flushes all memtables asynchronously
func (s *V12Store) FlushAllAsync() {
	go func() {
		if err := s.FlushAll(); err != nil {
			s.log("async flush error: %v", err)
		}
	}()
}

// FlushIfMemoryNeededAsync flushes if memory threshold exceeded (async)
func (s *V12Store) FlushIfMemoryNeededAsync(thresholdBytes int64) bool {
	if s.MemtableSize() >= thresholdBytes {
		s.FlushAllAsync()
		return true
	}
	return false
}

// parallelSortRecords sorts records in parallel using divide-and-conquer.
// It divides the slice into chunks, sorts each in parallel, then merges.
func parallelSortRecords(records []V12Record, numWorkers int) []V12Record {
	n := len(records)
	if n < 100000 || numWorkers <= 1 {
		// Small slices or single worker: just sort directly
		sort.Slice(records, func(i, j int) bool {
			return bytes.Compare(records[i].Key[:], records[j].Key[:]) < 0
		})
		return records
	}

	// Divide into chunks for parallel sorting
	chunkSize := (n + numWorkers - 1) / numWorkers
	chunks := make([][]V12Record, 0, numWorkers)

	for i := 0; i < n; i += chunkSize {
		end := i + chunkSize
		if end > n {
			end = n
		}
		chunks = append(chunks, records[i:end])
	}

	// Sort each chunk in parallel
	var wg sync.WaitGroup
	wg.Add(len(chunks))
	for i := range chunks {
		go func(idx int) {
			defer wg.Done()
			sort.Slice(chunks[idx], func(a, b int) bool {
				return bytes.Compare(chunks[idx][a].Key[:], chunks[idx][b].Key[:]) < 0
			})
		}(i)
	}
	wg.Wait()

	// K-way merge of sorted chunks
	return kWayMerge(chunks)
}

// kWayMerge merges k sorted slices into one sorted slice using parallel pairwise merging.
// This is O(n log k) and uses multiple cores for the merge phase.
func kWayMerge(chunks [][]V12Record) []V12Record {
	if len(chunks) == 0 {
		return nil
	}
	if len(chunks) == 1 {
		return chunks[0]
	}

	// Parallel pairwise merge: merge chunks in pairs, halving the number each round
	for len(chunks) > 1 {
		newChunks := make([][]V12Record, (len(chunks)+1)/2)

		var wg sync.WaitGroup
		for i := 0; i < len(chunks); i += 2 {
			idx := i / 2
			if i+1 >= len(chunks) {
				// Odd chunk, just copy
				newChunks[idx] = chunks[i]
				continue
			}

			wg.Add(1)
			go func(idx int, left, right []V12Record) {
				defer wg.Done()
				newChunks[idx] = mergeTwoSorted(left, right)
			}(idx, chunks[i], chunks[i+1])
		}
		wg.Wait()
		chunks = newChunks
	}

	return chunks[0]
}

// mergeTwoSorted merges two sorted slices into one sorted slice.
func mergeTwoSorted(a, b []V12Record) []V12Record {
	result := make([]V12Record, 0, len(a)+len(b))
	i, j := 0, 0

	for i < len(a) && j < len(b) {
		if bytes.Compare(a[i].Key[:], b[j].Key[:]) <= 0 {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}

	// Append remaining elements
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)

	return result
}
