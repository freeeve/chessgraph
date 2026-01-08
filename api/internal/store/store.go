package store

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/freeeve/chessgraph/api/internal/graph"
	"github.com/klauspost/compress/zstd"
)

// Config configures the Store
type Config struct {
	Dir                string
	TargetL0Size       int64  // target uncompressed L0 file size, default 256MB
	NumWorkers         int    // default runtime.NumCPU()
	L0Threshold        int    // trigger compaction when L0 files exceed this, default 5
	EvalFlushThreshold int64  // flush memtable after this many evals, default 1000 (0 = disabled)
	PositionCacheBytes int64  // bytes for position-level cache, default 0 (disabled)
	L2Compression      string // "fast" or "best" for L2 compression level (default "fast")
}

// Store is the main position store using LSM-tree architecture
type Store struct {
	dir   string
	l0Dir string
	l1Dir string
	l2Dir string

	// Indexes
	l1Index      *FileIndex
	l2BlockIndex *L2BlockIndex
	l2DataFiles  map[uint16]*os.File
	l2DataFilesMu sync.RWMutex

	// Encoders/decoders
	l0Encoder *zstd.Encoder
	l1Encoder *zstd.Encoder
	l2Encoder *zstd.Encoder
	decoder   *zstd.Decoder

	// Memory structures
	memtables       []*Memtable
	memtableMu      sync.Mutex
	memtableFlushMu []sync.Mutex // per-memtable flush locks

	// File counters
	l0FileCount     int64
	l1FileCount     int64
	l2DataFileCount uint16

	// Compaction state
	flushMu     sync.RWMutex
	compacting  int32 // atomic flag
	l0Threshold int

	// Background compaction
	compactStop chan struct{}
	compactDone chan struct{}
	stopping    int32 // atomic: set to 1 when shutdown requested

	// Caches (only position cache for hot lookups, no L0/L1 file caches)
	l0Index   *L0IndexCache
	posCache  *PositionCache
	evalCache *EvalCache // In-memory eval cache (set externally)

	// Config
	targetL0Size       int64
	evalFlushThreshold int64
	numWorkers         int

	// Stats
	stats *StatsCollector

	// Logging
	logFunc func(format string, args ...any)
}

// New creates a new Store
func New(cfg Config) (*Store, error) {
	// Apply defaults
	if cfg.TargetL0Size == 0 {
		cfg.TargetL0Size = 256 * 1024 * 1024 // 256MB default
	}
	if cfg.NumWorkers == 0 {
		cfg.NumWorkers = runtime.NumCPU()
	}
	if cfg.L0Threshold == 0 {
		cfg.L0Threshold = 5
	}
	if cfg.EvalFlushThreshold == 0 {
		cfg.EvalFlushThreshold = 1000
	}

	l0Dir := filepath.Join(cfg.Dir, "l0")
	l1Dir := filepath.Join(cfg.Dir, "l1")
	l2Dir := filepath.Join(cfg.Dir, "l2")

	// Create directories
	for _, dir := range []string{l0Dir, l1Dir, l2Dir} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	// Create encoders
	l0Encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}

	l1Encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return nil, err
	}

	// L2 uses best compression by default (final layer, written once)
	l2Level := zstd.SpeedBestCompression
	if cfg.L2Compression == "fast" {
		l2Level = zstd.SpeedFastest
	}
	l2Encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(l2Level))
	if err != nil {
		return nil, err
	}

	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))
	if err != nil {
		return nil, err
	}

	// Create memtables
	memtables := make([]*Memtable, cfg.NumWorkers)
	for i := range memtables {
		memtables[i] = NewMemtable()
	}

	s := &Store{
		dir:   cfg.Dir,
		l0Dir: l0Dir,
		l1Dir: l1Dir,
		l2Dir: l2Dir,

		l1Index:      NewFileIndex(),
		l2BlockIndex: NewL2BlockIndex(),
		l2DataFiles:  make(map[uint16]*os.File),

		l0Encoder: l0Encoder,
		l1Encoder: l1Encoder,
		l2Encoder: l2Encoder,
		decoder:   decoder,

		memtables:       memtables,
		memtableFlushMu: make([]sync.Mutex, cfg.NumWorkers),

		l0Threshold:        cfg.L0Threshold,
		targetL0Size:       cfg.TargetL0Size,
		evalFlushThreshold: cfg.EvalFlushThreshold,
		numWorkers:         cfg.NumWorkers,

		stats: NewStatsCollector(cfg.Dir),
	}

	// Initialize caches (only L0 index and position cache)
	s.l0Index = NewL0IndexCache()
	if cfg.PositionCacheBytes > 0 {
		s.posCache = NewPositionCache(cfg.PositionCacheBytes)
	}

	// Load existing L1 index
	if err := s.l1Index.LoadFromDir(l1Dir, decoder); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("load L1 index: %w", err)
	}

	// Load L2 block index
	l2IndexPath := filepath.Join(l2Dir, "l2_index.bin")
	l2BlockIdx, err := LoadL2Index(l2IndexPath)
	if err != nil {
		return nil, fmt.Errorf("load L2 block index: %w", err)
	}
	s.l2BlockIndex = l2BlockIdx

	// Open L2 data files
	if err := s.openL2DataFiles(); err != nil {
		return nil, fmt.Errorf("open L2 data files: %w", err)
	}

	// Load metadata
	if err := s.stats.LoadMetadata(); err != nil {
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	// Initialize file counters by scanning directories
	s.initFileCounters()

	// Initialize cached layer stats
	s.initCachedStats()

	return s, nil
}

// initFileCounters scans directories to find the highest file numbers
func (s *Store) initFileCounters() {
	// Scan L0 directory
	if entries, err := os.ReadDir(s.l0Dir); err == nil {
		for _, e := range entries {
			name := e.Name()
			if strings.HasPrefix(name, "l0_") && strings.HasSuffix(name, ".psv3") {
				numStr := name[3 : len(name)-5]
				if num, err := strconv.ParseInt(numStr, 10, 64); err == nil {
					if num > s.l0FileCount {
						s.l0FileCount = num
					}
				}
			}
		}
	}

	// Scan L1 directory
	if entries, err := os.ReadDir(s.l1Dir); err == nil {
		for _, e := range entries {
			name := e.Name()
			if strings.HasPrefix(name, "l1_") && strings.HasSuffix(name, ".psv3") {
				numStr := name[3 : len(name)-5]
				if num, err := strconv.ParseInt(numStr, 10, 64); err == nil {
					if num > s.l1FileCount {
						s.l1FileCount = num
					}
				}
			}
		}
	}

	// L2 data file count from index (max file ID, 0-indexed)
	if s.l2BlockIndex.Len() > 0 {
		s.l2DataFileCount = s.l2BlockIndex.MaxFileID()
	}
}

// initCachedStats scans directories to initialize layer statistics
func (s *Store) initCachedStats() {
	var l0Files, l0Positions, l0Compressed, l0Uncompressed int64
	var l1Files, l1Positions, l1Compressed, l1Uncompressed int64

	// Scan L0 directory
	if entries, err := os.ReadDir(s.l0Dir); err == nil {
		for _, e := range entries {
			if e.IsDir() || !isPositionFile(e.Name()) {
				continue
			}
			l0Files++
			path := filepath.Join(s.l0Dir, e.Name())
			if info, err := e.Info(); err == nil {
				l0Compressed += info.Size()
			}
			if header, err := ReadV13Header(path); err == nil {
				l0Positions += int64(header.RecordCount)
				keySuffixBytes := int64(header.RecordCount) * int64(header.KeySuffixSize)
				valueVarBytes := int64(header.RecordCount) * int64(header.ValueVarCount)
				l0Uncompressed += keySuffixBytes + valueVarBytes
			}
		}
	}
	s.stats.SetL0Stats(l0Files, l0Positions, l0Compressed, l0Uncompressed)

	// Scan L1 using index
	for _, f := range s.l1Index.Files() {
		l1Files++
		l1Positions += int64(f.Count)
		if info, err := os.Stat(f.Path); err == nil {
			l1Compressed += info.Size()
		}
		if header, err := ReadV13Header(f.Path); err == nil {
			keySuffixBytes := int64(header.RecordCount) * int64(header.KeySuffixSize)
			valueVarBytes := int64(header.RecordCount) * int64(header.ValueVarCount)
			l1Uncompressed += keySuffixBytes + valueVarBytes
		}
	}
	s.stats.SetL1Stats(l1Files, l1Positions, l1Compressed, l1Uncompressed)

	// Get L2 stats from index
	blocks, records, compressed := s.l2BlockIndex.Stats()
	uncompressed := records * RecordSize
	s.stats.SetL2Stats(blocks, records, compressed, uncompressed)
}

// isPositionFile returns true if the filename has a position store extension
func isPositionFile(name string) bool {
	return filepath.Ext(name) == ".psv3"
}

// openL2DataFiles opens all L2 data files referenced by the index
func (s *Store) openL2DataFiles() error {
	s.l2DataFilesMu.Lock()
	defer s.l2DataFilesMu.Unlock()

	fileIDs := make(map[uint16]bool)
	for i := 0; i < s.l2BlockIndex.Len(); i++ {
		entry := s.l2BlockIndex.Entry(i)
		if entry != nil {
			fileIDs[entry.FileID] = true
		}
	}

	for id := range fileIDs {
		path := filepath.Join(s.l2Dir, fmt.Sprintf("l2_data_%05d.bin", id))
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("open L2 data file %d: %w", id, err)
		}
		s.l2DataFiles[id] = f
	}

	return nil
}

// getL2DataFile returns an open file handle for the given file ID
func (s *Store) getL2DataFile(id uint16) (*os.File, error) {
	s.l2DataFilesMu.RLock()
	f, ok := s.l2DataFiles[id]
	s.l2DataFilesMu.RUnlock()
	if ok {
		return f, nil
	}

	// Try to open
	s.l2DataFilesMu.Lock()
	defer s.l2DataFilesMu.Unlock()

	// Double-check
	if f, ok := s.l2DataFiles[id]; ok {
		return f, nil
	}

	path := filepath.Join(s.l2Dir, fmt.Sprintf("l2_data_%05d.bin", id))
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	s.l2DataFiles[id] = f
	return f, nil
}

// Close closes the store and releases resources
func (s *Store) Close() error {
	// Stop background compaction
	s.StopBackgroundCompaction()

	// Flush all memtables
	if err := s.FlushAll(); err != nil {
		return err
	}

	// Save metadata
	if err := s.stats.SaveMetadata(); err != nil {
		return err
	}

	// Close L2 data files
	s.l2DataFilesMu.Lock()
	for _, f := range s.l2DataFiles {
		f.Close()
	}
	s.l2DataFiles = make(map[uint16]*os.File)
	s.l2DataFilesMu.Unlock()

	// Close encoders
	s.l0Encoder.Close()
	s.l1Encoder.Close()
	s.l2Encoder.Close()
	s.decoder.Close()

	return nil
}

// Get retrieves a record by position key (L2 stats + eval cache)
func (s *Store) Get(key graph.PositionKey) (*PositionRecord, error) {
	rec, _ := s.GetWithTiming(key)
	if rec == nil {
		return nil, ErrNotFound
	}
	return rec, nil
}

// GetWithTiming retrieves a record with timing information (L2 stats + eval cache)
func (s *Store) GetWithTiming(key graph.PositionKey) (*PositionRecord, GetTiming) {
	s.stats.IncrementReads()

	var k [KeySize]byte
	copy(k[:], key[:])

	var result *PositionRecord
	var timing GetTiming

	// Check position cache first
	if s.posCache != nil {
		if cached := s.posCache.Get(k); cached != nil {
			timing.CacheHit = true
			return cached, timing
		}
	}

	// Get stats from L2
	if blockIdx := s.l2BlockIndex.FindBlock(k); blockIdx >= 0 {
		entry := s.l2BlockIndex.Entry(blockIdx)
		if entry != nil {
			if f, err := s.getL2DataFile(entry.FileID); err == nil {
				if block, err := ReadL2Block(f, entry.Offset, int(entry.CompressedSize), s.decoder); err == nil {
					if rec, err := block.Get(k); err == nil && rec != nil {
						result = rec
					}
				}
			}
		}
	}

	// Merge eval data from cache (takes precedence for eval fields)
	if s.evalCache != nil {
		if eval, ok := s.evalCache.Get(k); ok {
			if result == nil {
				result = &PositionRecord{}
			}
			result.CP = eval.CP
			result.DTM = eval.DTM
			result.DTZ = eval.DTZ
			result.ProvenDepth = eval.ProvenDepth
			if eval.HasCP {
				result.SetHasCP(true)
			}
		}
	}

	// Cache the merged result
	if result != nil && s.posCache != nil {
		s.posCache.Put(k, result)
	}

	return result, timing
}

// SetEvalCache sets the eval cache for merging eval data with L2 stats.
func (s *Store) SetEvalCache(cache *EvalCache) {
	s.evalCache = cache
}

// GetFromAllLayers retrieves a record by checking all layers (memtable, L0, L1, L2).
// Used for testing and internal operations that need to see uncommitted data.
func (s *Store) GetFromAllLayers(key graph.PositionKey) (*PositionRecord, error) {
	var k [KeySize]byte
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

	// Check L0 files
	l0Recs := s.getFromL0(k)
	for _, rec := range l0Recs {
		if result == nil {
			result = &PositionRecord{}
			*result = rec
		} else {
			mergeRecords(result, &rec)
		}
	}

	// Check L1 using index
	if fileIdx := s.l1Index.FindFile(k); fileIdx >= 0 {
		files := s.l1Index.Files()
		if f, err := s.openL1File(files[fileIdx].Path); err == nil {
			if rec, err := f.Get(k); err == nil && rec != nil {
				if result == nil {
					result = rec
				} else {
					mergeRecords(result, rec)
				}
			}
		}
	}

	// Check L2 using block index
	if blockIdx := s.l2BlockIndex.FindBlock(k); blockIdx >= 0 {
		entry := s.l2BlockIndex.Entry(blockIdx)
		if entry != nil {
			if f, err := s.getL2DataFile(entry.FileID); err == nil {
				if block, err := ReadL2Block(f, entry.Offset, int(entry.CompressedSize), s.decoder); err == nil {
					if rec, err := block.Get(k); err == nil && rec != nil {
						if result == nil {
							result = rec
						} else {
							mergeRecords(result, rec)
						}
					}
				}
			}
		}
	}

	if result == nil {
		return nil, ErrNotFound
	}
	return result, nil
}

// getFromL0 retrieves all matching records from L0 files
func (s *Store) getFromL0(key [KeySize]byte) []PositionRecord {
	entries, err := os.ReadDir(s.l0Dir)
	if err != nil {
		return nil
	}

	var results []PositionRecord

	for _, e := range entries {
		if e.IsDir() || !isPositionFile(e.Name()) {
			continue
		}

		path := filepath.Join(s.l0Dir, e.Name())

		// Check bloom filter first
		if idx := s.l0Index.Get(path); idx != nil && !idx.MayContain(key) {
			continue
		}

		f, _ := s.openL0File(path)
		if f == nil {
			continue
		}

		if rec, err := f.Get(key); err == nil && rec != nil {
			results = append(results, *rec)
		}
	}

	return results
}

// openL0File opens an L0 file and builds bloom filter index if needed
func (s *Store) openL0File(path string) (PositionFile, error) {
	f, err := OpenPositionFile(path, s.decoder)
	if err != nil {
		return nil, err
	}

	// Build bloom filter index for fast rejection
	if s.l0Index.Get(path) == nil {
		records := make([]Record, 0, f.RecordCount())
		iter := f.Iterator()
		for {
			rec := iter.Next()
			if rec == nil {
				break
			}
			records = append(records, *rec)
		}
		if idx := NewL0FileIndex(records); idx != nil {
			s.l0Index.Put(path, idx)
		}
	}

	return f, nil
}

// openL1File opens an L1 file directly (no caching)
func (s *Store) openL1File(path string) (PositionFile, error) {
	return OpenPositionFile(path, s.decoder)
}

// Put writes a record to the store
func (s *Store) Put(key graph.PositionKey, rec *PositionRecord) error {
	s.stats.IncrementWrites()

	var k [KeySize]byte
	copy(k[:], key[:])

	// Write to first memtable (or use worker-based routing)
	s.memtables[0].Put(k, rec)

	// Invalidate cache
	if s.posCache != nil {
		s.posCache.Invalidate(k)
	}

	return nil
}

// Increment adds W/D/L counts to a position
func (s *Store) Increment(key graph.PositionKey, wins, draws, losses uint16) {
	s.stats.IncrementWrites()

	var k [KeySize]byte
	copy(k[:], key[:])

	s.memtables[0].Increment(k, wins, draws, losses)

	if s.posCache != nil {
		s.posCache.Invalidate(k)
	}
}

// IncrementWorker adds W/D/L counts using a specific worker's memtable
func (s *Store) IncrementWorker(workerID int, key [KeySize]byte, wins, draws, losses uint16) {
	s.stats.IncrementWrites()
	s.memtables[workerID%len(s.memtables)].Increment(key, wins, draws, losses)

	if s.posCache != nil {
		s.posCache.Invalidate(key)
	}
}

// IncrementGameCount adds to the total game count
func (s *Store) IncrementGameCount(n uint64) {
	s.stats.IncrementGames(n)
}

// Stats returns current store statistics
func (s *Store) Stats() Stats {
	stats := s.stats.Stats()

	// Add memtable stats
	var memtablePositions int
	var memtableBytes int64
	for _, mt := range s.memtables {
		memtablePositions += mt.Count()
		memtableBytes += mt.Size()
	}
	stats.MemtablePositions = uint64(memtablePositions)
	stats.MemtableBytes = memtableBytes

	// Add L2 file count (number of unique l2_data_*.bin files in index)
	stats.L2Files = s.l2BlockIndex.UniqueFileCount()

	// Add eval stats from eval cache
	if s.evalCache != nil {
		evalStats := s.evalCache.Stats()
		stats.EvaluatedPositions = uint64(evalStats.Total)
		stats.CPPositions = uint64(evalStats.CP)
		stats.DTMPositions = uint64(evalStats.DTM)
		stats.DTZPositions = uint64(evalStats.DTZ)
	}

	// Calculate total
	stats.TotalPositions = stats.MemtablePositions + stats.L0Positions + stats.L1Positions + stats.L2Positions

	return stats
}

// IsReadOnly returns false (this store is writable)
func (s *Store) IsReadOnly() bool {
	return false
}

// NumWorkers returns the number of worker memtables
func (s *Store) NumWorkers() int {
	return s.numWorkers
}

// L0FileCount returns the current number of L0 files
func (s *Store) L0FileCount() int {
	return s.countL0Files()
}

// countL0Files counts the number of L0 files
func (s *Store) countL0Files() int {
	entries, err := os.ReadDir(s.l0Dir)
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && isPositionFile(e.Name()) {
			count++
		}
	}
	return count
}

// SetLogger sets the logging function
func (s *Store) SetLogger(f func(format string, args ...any)) {
	s.logFunc = f
}

func (s *Store) log(format string, args ...any) {
	if s.logFunc != nil {
		s.logFunc(format, args...)
	}
}

// mergeRecords merges src into dst
func mergeRecords(dst, src *PositionRecord) {
	// Sum W/D/L if neither is at max
	if dst.Wins < MaxWDLCount && dst.Draws < MaxWDLCount && dst.Losses < MaxWDLCount &&
		src.Wins < MaxWDLCount && src.Draws < MaxWDLCount && src.Losses < MaxWDLCount {
		dst.Wins = SaturatingAdd16(dst.Wins, src.Wins)
		dst.Draws = SaturatingAdd16(dst.Draws, src.Draws)
		dst.Losses = SaturatingAdd16(dst.Losses, src.Losses)
	}

	// Keep eval from src if it has one
	if src.HasCP() {
		dst.CP = src.CP
		dst.DTM = src.DTM
		dst.DTZ = src.DTZ
		dst.ProvenDepth = src.ProvenDepth
		dst.SetHasCP(true)
	}
}

// clearL0Cache clears L0 index after compaction
func (s *Store) clearL0Cache() {
	s.l0Index.Clear()
}

// clearL1Cache is a no-op (no L1 cache)
func (s *Store) clearL1Cache() {
}

// nextL0FileName returns the next L0 file name
func (s *Store) nextL0FileName() string {
	num := atomic.AddInt64(&s.l0FileCount, 1)
	return filepath.Join(s.l0Dir, fmt.Sprintf("l0_%06d.psv3", num))
}

// nextL1FileName returns the next L1 file name
func (s *Store) nextL1FileName() string {
	num := atomic.AddInt64(&s.l1FileCount, 1)
	return filepath.Join(s.l1Dir, fmt.Sprintf("l1_%09d.psv3", num))
}

// l2IndexPath returns the path to the L2 index file
func (s *Store) l2IndexPath() string {
	return filepath.Join(s.l2Dir, "l2_index.bin")
}

// IterateAll iterates over all records in the store (L0, L1, L2)
// The callback receives the key and record for each position.
// Return false from the callback to stop iteration.
func (s *Store) IterateAll(fn func(key [KeySize]byte, rec *PositionRecord) bool) error {
	// Iterate L0 files first
	l0Entries, err := os.ReadDir(s.l0Dir)
	if err == nil {
		for _, e := range l0Entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".psv3") {
				continue
			}
			path := filepath.Join(s.l0Dir, e.Name())
			f, err := OpenPositionFile(path, s.decoder)
			if err != nil {
				return fmt.Errorf("open L0 file %s: %w", path, err)
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
	}

	// Iterate L1 files
	for _, fileRange := range s.l1Index.Files() {
		f, err := OpenPositionFile(fileRange.Path, s.decoder)
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

	// Iterate L2 blocks
	l2Iter := NewL2BlockIndexIterator(s.l2BlockIndex, s.getL2DataFile, s.decoder)
	for {
		rec := l2Iter.Next()
		if rec == nil {
			break
		}
		if !fn(rec.Key, &rec.Value) {
			return nil
		}
	}

	return nil
}

// Backwards compatibility aliases
type V12StoreConfig = Config
type V12Store = Store

// NewV12Store is an alias for New (backwards compatibility)
func NewV12Store(cfg V12StoreConfig) (*Store, error) {
	return New(cfg)
}
