// Package store provides position-keyed storage for chess positions.
//
// TODO:
// - Memory-mapped reads for read-only mode
// - Block size limits (split huge blocks, merge tiny ones)
// - Two-tier storage (hot/cold) for frequently accessed positions
package store

import (
	"bytes"
	"container/list"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/singleflight"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// Position store constants
const (
	// V10 block structure: simple byte-based paths from 26-byte packed position
	// - Level 1: {xx}.block - key: 25 bytes (first byte in path)
	// - Level 2: {xx}/{yy}.block - key: 24 bytes
	// - Level N: path has N bytes, key has 26-N bytes
	// - Level 25: max depth, key is 1 byte
	// Blocks split dynamically when they exceed MaxBlockSizeBytes

	// MaxBlockLevel is the deepest level (one byte per level from 26-byte packed position)
	MaxBlockLevel = 25

	// MaxBlockKeySize is the maximum block key size (26-byte packed position at level 0)
	MaxBlockKeySize = 26

	// DirtyEntrySize estimates actual memory per dirty map entry including Go map overhead
	DirtyEntrySize = 100

	// PSMetaFileName is the metadata file name
	PSMetaFileName = "posstore.meta"

	// PSMaxCachedBlocks is the default number of decompressed block files to cache
	PSMaxCachedBlocks = 1024

	// Block file format constants
	PSBlockMagic      = "CGPK"    // ChessGraph Position Block
	PSBlockVersionV10 = uint8(10) // V10: columnar data, contiguous keys
	PSBlockVersionV11 = uint8(11) // V11: columnar data, striped keys (better compression)
	PSBlockVersion    = PSBlockVersionV11 // Current write version
	PSBlockHeaderSize = 17       // Magic(4) + Version(1) + Flags(1) + Level(1) + RecordCount(4) + Checksum(4) + Reserved(2)

	// MaxBlockSizeBytes is the target maximum uncompressed block size (64MB)
	// Blocks exceeding this will be split to the next level
	MaxBlockSizeBytes = 64 * 1024 * 1024
)

const (
	// PSLockFileName is the lock file name indicating an ingest is in progress
	PSLockFileName = ".ingest.lock"
)

var (
	ErrPSKeyNotFound = errors.New("position not found")
	ErrPSInvalidKey  = errors.New("invalid position key")
	ErrPSReadOnly    = errors.New("position store is read-only")
)

// Types PSPrefix, BlockKey, BlockRecord, LeveledBlockRecord
// and functions BlockKeySizeAtLevel, BlockRecordSizeAtLevel are in positionstore_keys.go

// PositionStore implements position-keyed storage for chess positions.
// Positions are organized by:
// - Folder: king positions
// - Filename: pawn bitboards + castle rights + EP file
// - In-file: sorted by remaining pieces (suffix)
type PositionStore struct {
	dir      string
	encoder  *zstd.Encoder
	decoder  *zstd.Decoder
	readOnly bool // If true, writes are rejected

	// LRU cache for decompressed block files
	cache     map[string]*list.Element // filename -> list element
	cacheList *list.List
	cacheMu   sync.RWMutex
	maxCached int

	// Dirty tracking for writes - append-only slices, sorted on demand
	dirtyBlocks    map[string]*dirtyBlock   // filename -> dirty block
	dirtyBytes     int64                    // approximate memory used by dirty blocks
	inflightBlocks map[string][]BlockRecord // filename -> records being flushed
	dirtyMu        sync.Mutex

	// File I/O
	fileMu sync.RWMutex

	// Stats
	totalReads  uint64
	totalWrites uint64

	// Metadata (loaded from disk)
	meta   PSMeta
	metaMu sync.RWMutex

	// Background flush state
	flushMu      sync.Mutex
	flushRunning bool

	log func(format string, args ...any)

	// Configurable split threshold (defaults to MaxBlockSizeBytes)
	maxBlockSizeBytes uint64

	// Path resolution cache - avoids repeated os.Stat calls during ingestion
	pathCache   map[uint64]pathCacheEntry // prefix hash -> resolved path/level
	pathCacheMu sync.RWMutex

	// Singleflight for block loading - prevents duplicate disk reads
	blockLoadGroup singleflight.Group

	// Eval log - appends eval data to CSV file during flush
	evalLogPath    string              // path to eval CSV file (empty = disabled)
	pendingEvals   []pendingEvalEntry  // buffered eval entries waiting for flush
	pendingEvalsMu sync.Mutex
}

// pendingEvalEntry stores an eval to be written to the CSV log
type pendingEvalEntry struct {
	pos    graph.PositionKey
	record PositionRecord
}

// pathCacheEntry stores a cached path resolution result
type pathCacheEntry struct {
	path  string
	level int
}

// psBlockCache stores decompressed block data
type psBlockCache struct {
	filename string
	records  []BlockRecord
	level    int // block level (determines key size)
}

// psCacheEntry is an LRU cache entry
type psCacheEntry struct {
	filename string
	block    *psBlockCache
}

// dirtyBlock is an append-only list of records for a block file.
// Records are sorted and compacted on-demand (for lookups or flush).
type dirtyBlock struct {
	records []BlockRecord // append-only until compacted
	sorted  bool          // true if sorted and compacted (no duplicates)
	level   int           // block level (determines key size)
}

// keySize returns the key size for this dirty block
func (db *dirtyBlock) keySize() int {
	return BlockKeySizeAtLevel(db.level)
}

// compact sorts records and merges duplicates.
// After compaction, sorted=true and there are no duplicate keys.
func (db *dirtyBlock) compact() {
	if db.sorted || len(db.records) <= 1 {
		db.sorted = true
		return
	}

	keySize := db.keySize()

	// Sort by key (only compare relevant bytes based on level)
	sort.Slice(db.records, func(i, j int) bool {
		return bytes.Compare(db.records[i].Key[:keySize], db.records[j].Key[:keySize]) < 0
	})

	// Merge duplicates in place
	// For same key: sum counts, take last non-zero eval fields
	w := 0 // write index
	for r := 0; r < len(db.records); r++ {
		if w > 0 && bytes.Equal(db.records[w-1].Key[:keySize], db.records[r].Key[:keySize]) {
			// Merge with previous
			prev := &db.records[w-1]
			curr := &db.records[r]

			// Skip count updates if any field is already saturated (preserves ratios)
			if prev.Data.Wins < 65535 && prev.Data.Draws < 65535 && prev.Data.Losses < 65535 {
				// Sum counts with clamping at 65535
				newWins := uint32(prev.Data.Wins) + uint32(curr.Data.Wins)
				if newWins > 65535 {
					newWins = 65535
				}
				newDraws := uint32(prev.Data.Draws) + uint32(curr.Data.Draws)
				if newDraws > 65535 {
					newDraws = 65535
				}
				newLosses := uint32(prev.Data.Losses) + uint32(curr.Data.Losses)
				if newLosses > 65535 {
					newLosses = 65535
				}
				prev.Data.Wins = uint16(newWins)
				prev.Data.Draws = uint16(newDraws)
				prev.Data.Losses = uint16(newLosses)
			}

			// Take eval fields from current if it has eval data
			if curr.Data.HasCP() || curr.Data.DTM != DTMUnknown || curr.Data.DTZ != 0 {
				prev.Data.CP = curr.Data.CP
				prev.Data.DTM = curr.Data.DTM
				prev.Data.DTZ = curr.Data.DTZ
				prev.Data.ProvenDepth = curr.Data.ProvenDepth
			}
		} else {
			// New key
			if w != r {
				db.records[w] = db.records[r]
			}
			w++
		}
	}
	db.records = db.records[:w]
	db.sorted = true
}

// lookup finds a record by key, returns nil if not found.
// Compacts if needed.
func (db *dirtyBlock) lookup(key BlockKey) *PositionRecord {
	db.compact()
	keySize := db.keySize()
	idx := sort.Search(len(db.records), func(i int) bool {
		return bytes.Compare(db.records[i].Key[:keySize], key[:keySize]) >= 0
	})
	if idx < len(db.records) && bytes.Equal(db.records[idx].Key[:keySize], key[:keySize]) {
		return &db.records[idx].Data
	}
	return nil
}

// append adds a record (invalidates sorted state)
func (db *dirtyBlock) append(rec BlockRecord) {
	db.records = append(db.records, rec)
	db.sorted = false
}

// NewPositionStore creates or opens a position store
func NewPositionStore(dir string, maxCached int) (*PositionStore, error) {
	if maxCached <= 0 {
		maxCached = PSMaxCachedBlocks
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create position store dir: %w", err)
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedBestCompression))
	if err != nil {
		return nil, fmt.Errorf("create zstd encoder: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		encoder.Close()
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}

	ps := &PositionStore{
		dir:               dir,
		encoder:           encoder,
		decoder:           decoder,
		cache:             make(map[string]*list.Element),
		cacheList:         list.New(),
		maxCached:         maxCached,
		dirtyBlocks:       make(map[string]*dirtyBlock),
		inflightBlocks:    make(map[string][]BlockRecord),
		log:               func(format string, args ...any) {},
		maxBlockSizeBytes: MaxBlockSizeBytes,
		pathCache:         make(map[uint64]pathCacheEntry),
	}

	// Load metadata if it exists
	if err := ps.LoadMeta(); err != nil {
		encoder.Close()
		decoder.Close()
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	// Enable eval logging by default
	evalLogPath := filepath.Join(dir, "eval_log.csv")
	if err := ps.SetEvalLogPath(evalLogPath); err != nil {
		// Non-fatal, just log and continue
		ps.log("warning: failed to enable eval logging: %v", err)
	}

	return ps, nil
}

// SetLogger sets a logging function
func (ps *PositionStore) SetLogger(log func(format string, args ...any)) {
	ps.log = log
}

// SetMaxBlockSize sets the maximum uncompressed block size threshold for splitting.
// Use this for testing with smaller thresholds. Defaults to MaxBlockSizeBytes (128MB).
func (ps *PositionStore) SetMaxBlockSize(size uint64) {
	ps.maxBlockSizeBytes = size
}

// SetEvalLogPath enables eval CSV logging. When set, all evals written via Put()
// will be appended to the specified CSV file during FlushAll().
// The CSV format is: fen,position,cp,dtm,dtz,proven_depth
// If the file doesn't exist, it will be created with a header row.
func (ps *PositionStore) SetEvalLogPath(path string) error {
	if ps.readOnly {
		return ErrPSReadOnly
	}
	ps.evalLogPath = path

	// Create file with header if it doesn't exist
	if _, err := os.Stat(path); os.IsNotExist(err) {
		f, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create eval log: %w", err)
		}
		if _, err := f.WriteString("fen,position,cp,dtm,dtz,proven_depth\n"); err != nil {
			f.Close()
			return fmt.Errorf("write eval log header: %w", err)
		}
		f.Close()
	}
	return nil
}

// NewPositionStoreReadOnly opens a position store in read-only mode.
// This is safe to use while another process is writing to the store.
func NewPositionStoreReadOnly(dir string, maxCached int) (*PositionStore, error) {
	if maxCached <= 0 {
		maxCached = PSMaxCachedBlocks
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}

	ps := &PositionStore{
		dir:               dir,
		decoder:           decoder,
		readOnly:          true,
		cache:             make(map[string]*list.Element),
		cacheList:         list.New(),
		maxCached:         maxCached,
		dirtyBlocks:       make(map[string]*dirtyBlock),
		inflightBlocks:    make(map[string][]BlockRecord),
		log:               func(format string, args ...any) {},
		maxBlockSizeBytes: MaxBlockSizeBytes,
		pathCache:         make(map[uint64]pathCacheEntry),
	}

	// Load metadata if it exists
	if err := ps.LoadMeta(); err != nil {
		decoder.Close()
		return nil, fmt.Errorf("load metadata: %w", err)
	}

	return ps, nil
}

// IsReadOnly returns true if the store is in read-only mode
func (ps *PositionStore) IsReadOnly() bool {
	return ps.readOnly
}

// LockFilePath returns the full path to the lock file
func (ps *PositionStore) LockFilePath() string {
	return filepath.Join(ps.dir, PSLockFileName)
}

// IsLocked checks if an ingest lock file exists
func (ps *PositionStore) IsLocked() bool {
	_, err := os.Stat(ps.LockFilePath())
	return err == nil
}

// AcquireLock creates the ingest lock file. Returns error if already locked.
func (ps *PositionStore) AcquireLock() error {
	lockPath := ps.LockFilePath()
	if _, err := os.Stat(lockPath); err == nil {
		return fmt.Errorf("lock file already exists: %s", lockPath)
	}

	// Write lock file with PID and timestamp
	content := fmt.Sprintf("pid=%d\ntime=%s\n", os.Getpid(), time.Now().Format(time.RFC3339))
	if err := os.WriteFile(lockPath, []byte(content), 0644); err != nil {
		return fmt.Errorf("create lock file: %w", err)
	}
	return nil
}

// ReleaseLock removes the ingest lock file
func (ps *PositionStore) ReleaseLock() error {
	lockPath := ps.LockFilePath()
	if err := os.Remove(lockPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove lock file: %w", err)
	}
	return nil
}

// CheckLock checks if a lock file exists and returns the directory path for external checking
func CheckLockFile(dir string) bool {
	lockPath := filepath.Join(dir, PSLockFileName)
	_, err := os.Stat(lockPath)
	return err == nil
}

// Close flushes all pending writes, saves metadata, and closes resources
func (ps *PositionStore) Close() error {
	if !ps.readOnly {
		// Only flush if there's dirty data or an async flush is in progress
		if ps.DirtyCount() > 0 || ps.IsFlushRunning() {
			if err := ps.FlushAll(); err != nil {
				if ps.encoder != nil {
					ps.encoder.Close()
				}
				ps.decoder.Close()
				return err
			}
		}
		// Save metadata before closing
		if err := ps.SaveMeta(); err != nil {
			ps.log("warning: failed to save metadata on close: %v", err)
		}
		if ps.encoder != nil {
			ps.encoder.Close()
		}
	}
	ps.decoder.Close()
	return nil
}

// Key extraction and path building functions are in positionstore_keys.go:
// ExtractPrefix, ExtractBlockKeyAtLevel, PSPrefix.PathAtLevel, etc.

// ResolveBlockPath finds the block file path for a prefix by traversing the trie.
// Returns the path and level where the block exists (or should be created).
// If no block exists, returns level 1 path.
// Uses a cache to avoid repeated os.Stat calls during ingestion.
func (ps *PositionStore) ResolveBlockPath(prefix PSPrefix) (path string, level int) {
	// Check cache first
	hash := prefix.Hash()
	ps.pathCacheMu.RLock()
	if entry, ok := ps.pathCache[hash]; ok {
		ps.pathCacheMu.RUnlock()
		return entry.path, entry.level
	}
	ps.pathCacheMu.RUnlock()

	// Cache miss - do the expensive lookup
	path, level = ps.resolveBlockPathUncached(prefix)

	// Store in cache
	ps.pathCacheMu.Lock()
	ps.pathCache[hash] = pathCacheEntry{path: path, level: level}
	ps.pathCacheMu.Unlock()

	return path, level
}

// resolveBlockPathUncached does the actual path resolution without caching.
// V10: Simplified byte-based paths - check each level until we find a block file or no directory.
func (ps *PositionStore) resolveBlockPathUncached(prefix PSPrefix) (path string, level int) {
	// Check from level 1 upward until we find a block file (not a directory)
	for level = 1; level <= MaxBlockLevel; level++ {
		path = prefix.PathAtLevel(level)
		fullPath := filepath.Join(ps.dir, path)

		info, err := os.Stat(fullPath)
		if err == nil && !info.IsDir() {
			// Found a block file
			return path, level
		}

		// Check if this level's path is a directory (meaning we need to go deeper)
		dirPath := strings.TrimSuffix(fullPath, ".block")
		dirInfo, err := os.Stat(dirPath)
		if err != nil || !dirInfo.IsDir() {
			// No directory at this level - block should be created here
			return path, level
		}
		// Directory exists, continue to next level
	}

	// Reached max level
	return prefix.PathAtLevel(MaxBlockLevel), MaxBlockLevel
}

// InvalidatePathCache clears the path resolution cache.
// Should be called after block splits or other structural changes.
func (ps *PositionStore) InvalidatePathCache() {
	ps.pathCacheMu.Lock()
	ps.pathCache = make(map[uint64]pathCacheEntry)
	ps.pathCacheMu.Unlock()
}

// ExtractBlockKeyAtLevel, PSPrefix.FileName, PSPrefix.Path are in positionstore_keys.go

// Get retrieves a position record by its key.
// Dirty records are merged with disk data to ensure we don't lose eval
// data when only counts are updated in the dirty block.
func (ps *PositionStore) Get(pos graph.PositionKey) (*PositionRecord, error) {
	// Extract all position data in a single pass through the board
	pd := ExtractPositionData(pos)
	prefix := pd.BuildPrefix()

	// Resolve the block path and level
	path, level := ps.ResolveBlockPath(prefix)

	// Build key from pre-extracted data (no second board scan)
	keyBytes := pd.BuildBlockKeyAtLevel(level)
	var blockKey BlockKey
	copy(blockKey[:], keyBytes)
	keySize := BlockKeySizeAtLevel(level)

	// Check dirty buffer and inflight (sorts and compacts on demand)
	var dirtyRec *PositionRecord
	ps.dirtyMu.Lock()
	if db, ok := ps.dirtyBlocks[path]; ok {
		if rec := db.lookup(blockKey); rec != nil {
			r := *rec // copy
			dirtyRec = &r
		}
	}
	// Check inflight records (being flushed to disk)
	if dirtyRec == nil {
		if records, ok := ps.inflightBlocks[path]; ok {
			idx := sort.Search(len(records), func(i int) bool {
				return bytes.Compare(records[i].Key[:keySize], blockKey[:keySize]) >= 0
			})
			if idx < len(records) && bytes.Equal(records[idx].Key[:keySize], blockKey[:keySize]) {
				r := records[idx].Data // copy
				dirtyRec = &r
			}
		}
	}
	ps.dirtyMu.Unlock()

	// Load block file to get disk data
	block, err := ps.getBlockFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	// Find on disk
	var diskRec *PositionRecord
	if block != nil {
		idx := sort.Search(len(block.records), func(i int) bool {
			return bytes.Compare(block.records[i].Key[:keySize], blockKey[:keySize]) >= 0
		})
		if idx < len(block.records) && bytes.Equal(block.records[idx].Key[:keySize], blockKey[:keySize]) {
			r := block.records[idx].Data // copy
			diskRec = &r
		}
	}

	// Merge dirty and disk data
	if dirtyRec == nil && diskRec == nil {
		return nil, ErrPSKeyNotFound
	}
	if dirtyRec == nil {
		return diskRec, nil
	}
	if diskRec == nil {
		return dirtyRec, nil
	}

	// Both exist - merge: sum counts, keep eval from dirty if it has one, else disk
	merged := *diskRec
	// Skip count updates if any field is already saturated (preserves ratios)
	if merged.Wins < 65535 && merged.Draws < 65535 && merged.Losses < 65535 {
		newWins := int(merged.Wins) + int(dirtyRec.Wins)
		if newWins > 65535 {
			newWins = 65535
		}
		newDraws := int(merged.Draws) + int(dirtyRec.Draws)
		if newDraws > 65535 {
			newDraws = 65535
		}
		newLosses := int(merged.Losses) + int(dirtyRec.Losses)
		if newLosses > 65535 {
			newLosses = 65535
		}
		merged.Wins = uint16(newWins)
		merged.Draws = uint16(newDraws)
		merged.Losses = uint16(newLosses)
	}
	if dirtyRec.HasCP() || dirtyRec.DTM != DTMUnknown || dirtyRec.DTZ != 0 {
		merged.CP = dirtyRec.CP
		merged.DTM = dirtyRec.DTM
		merged.DTZ = dirtyRec.DTZ
		merged.ProvenDepth = dirtyRec.ProvenDepth
	}
	return &merged, nil
}

// Put stores or updates a position record
func (ps *PositionStore) Put(pos graph.PositionKey, record *PositionRecord) error {
	if ps.readOnly {
		return ErrPSReadOnly
	}

	// Extract all position data in a single pass through the board
	pd := ExtractPositionData(pos)
	prefix := pd.BuildPrefix()

	// Resolve the block path and level
	path, level := ps.ResolveBlockPath(prefix)

	// Build key from pre-extracted data (no second board scan)
	keyBytes := pd.BuildBlockKeyAtLevel(level)
	var blockKey BlockKey
	copy(blockKey[:], keyBytes)

	ps.dirtyMu.Lock()
	defer ps.dirtyMu.Unlock()

	// Get or create the dirty block for this path
	db := ps.dirtyBlocks[path]
	if db == nil {
		db = &dirtyBlock{records: make([]BlockRecord, 0, 64), level: level}
		ps.dirtyBlocks[path] = db
		ps.dirtyBytes += int64(len(path)) + 50 // outer map entry overhead
	}

	// Append record - merging happens at compact time
	db.append(BlockRecord{Key: blockKey, Data: *record})
	ps.dirtyBytes += DirtyEntrySize
	ps.totalWrites++

	// Track eval for CSV log if enabled and this record has eval data
	if ps.evalLogPath != "" && (record.HasCP() || record.DTM != DTMUnknown || record.DTZ != 0) {
		ps.pendingEvalsMu.Lock()
		ps.pendingEvals = append(ps.pendingEvals, pendingEvalEntry{pos: pos, record: *record})
		ps.pendingEvalsMu.Unlock()
	}

	return nil
}

// Increment adds to position counts without reading from disk.
// This is much faster for ingest as it avoids disk I/O.
// The counts are merged with existing data at flush time.
// wins/draws/losses are incremented (clamped at 65535)
func (ps *PositionStore) Increment(pos graph.PositionKey, wins, draws, losses uint16) error {
	if ps.readOnly {
		return ErrPSReadOnly
	}

	// Extract all position data in a single pass through the board
	pd := ExtractPositionData(pos)
	prefix := pd.BuildPrefix()

	// Resolve the block path and level
	path, level := ps.ResolveBlockPath(prefix)

	// Build key from pre-extracted data (no second board scan)
	keyBytes := pd.BuildBlockKeyAtLevel(level)
	var blockKey BlockKey
	copy(blockKey[:], keyBytes)

	ps.dirtyMu.Lock()
	defer ps.dirtyMu.Unlock()

	// Get or create the dirty block for this path
	db := ps.dirtyBlocks[path]
	if db == nil {
		db = &dirtyBlock{records: make([]BlockRecord, 0, 64), level: level}
		ps.dirtyBlocks[path] = db
		ps.dirtyBytes += int64(len(path)) + 50 // outer map entry overhead
	}

	// Append record with counts - merging happens at compact time
	db.append(BlockRecord{
		Key:  blockKey,
		Data: PositionRecord{Wins: wins, Draws: draws, Losses: losses},
	})
	ps.dirtyBytes += DirtyEntrySize
	ps.totalWrites++

	return nil
}

// getBlockFile retrieves a pack file from cache or disk
func (ps *PositionStore) getBlockFile(filename string) (*psBlockCache, error) {
	// Check cache first (read-only, no LRU update for speed)
	ps.cacheMu.RLock()
	if elem, ok := ps.cache[filename]; ok {
		block := elem.Value.(*psCacheEntry).block
		ps.cacheMu.RUnlock()
		return block, nil
	}
	ps.cacheMu.RUnlock()

	// Use singleflight to prevent duplicate disk reads for the same block
	result, err, _ := ps.blockLoadGroup.Do(filename, func() (interface{}, error) {
		// Double-check cache (another goroutine might have loaded it)
		ps.cacheMu.RLock()
		if elem, ok := ps.cache[filename]; ok {
			block := elem.Value.(*psCacheEntry).block
			ps.cacheMu.RUnlock()
			return block, nil
		}
		ps.cacheMu.RUnlock()

		// Load from disk
		records, err := ps.loadBlockFile(filename)
		if err != nil {
			return nil, err
		}

		pack := &psBlockCache{
			filename: filename,
			records:  records,
		}

		// Add to cache
		ps.addToBlockCache(filename, pack)
		ps.totalReads++

		return pack, nil
	})

	if err != nil {
		return nil, err
	}
	return result.(*psBlockCache), nil
}

// addToBlockCache adds a pack to the LRU cache
func (ps *PositionStore) addToBlockCache(filename string, pack *psBlockCache) {
	ps.cacheMu.Lock()
	defer ps.cacheMu.Unlock()

	// Check if already in cache
	if elem, ok := ps.cache[filename]; ok {
		ps.cacheList.MoveToFront(elem)
		elem.Value.(*psCacheEntry).block = pack
		return
	}

	// Add new entry
	entry := &psCacheEntry{
		filename: filename,
		block:    pack,
	}
	elem := ps.cacheList.PushFront(entry)
	ps.cache[filename] = elem

	// Evict oldest if over limit
	for ps.cacheList.Len() > ps.maxCached {
		oldest := ps.cacheList.Back()
		if oldest != nil {
			ps.cacheList.Remove(oldest)
			oldEntry := oldest.Value.(*psCacheEntry)
			delete(ps.cache, oldEntry.filename)
		}
	}
}

// invalidateBlockCache removes a filename from pack cache
func (ps *PositionStore) invalidateBlockCache(filename string) {
	ps.cacheMu.Lock()
	defer ps.cacheMu.Unlock()

	if elem, ok := ps.cache[filename]; ok {
		ps.cacheList.Remove(elem)
		delete(ps.cache, filename)
	}
}

// encodeBlockRecordsAtLevel encodes records using V11 striped format for better compression.
// V11 Layout: Keys striped by byte position, then data fields columnar
// [keyByte0×N][keyByte1×N]...[keyByteK×N][wins×N×2][draws×N×2][losses×N×2][cp×N×2][dtm×N×2][dtz×N×2][provenDepth×N×2]
func encodeBlockRecordsAtLevel(records []BlockRecord, level int) []byte {
	n := len(records)
	if n == 0 {
		return nil
	}

	keySize := BlockKeySizeAtLevel(level)
	recordSize := keySize + positionRecordSize
	data := make([]byte, n*recordSize)

	// V11: Striped key layout - group by byte position for better compression
	// Since keys are sorted, adjacent keys share prefixes, so same-position bytes
	// across keys are often identical or similar
	for b := 0; b < keySize; b++ {
		off := b * n // Offset for this byte position
		for i, rec := range records {
			data[off+i] = rec.Key[b]
		}
	}

	// Data columns (same as V10)
	offData := n * keySize
	offWins := offData
	offDraws := offWins + n*2
	offLosses := offDraws + n*2
	offCP := offLosses + n*2
	offDTM := offCP + n*2
	offDTZ := offDTM + n*2
	offProvenDepth := offDTZ + n*2

	for i, rec := range records {
		binary.BigEndian.PutUint16(data[offWins+i*2:], rec.Data.Wins)
		binary.BigEndian.PutUint16(data[offDraws+i*2:], rec.Data.Draws)
		binary.BigEndian.PutUint16(data[offLosses+i*2:], rec.Data.Losses)
		binary.BigEndian.PutUint16(data[offCP+i*2:], uint16(rec.Data.CP))
		binary.BigEndian.PutUint16(data[offDTM+i*2:], uint16(rec.Data.DTM))
		binary.BigEndian.PutUint16(data[offDTZ+i*2:], rec.Data.DTZ)
		binary.BigEndian.PutUint16(data[offProvenDepth+i*2:], rec.Data.ProvenDepth)
	}

	return data
}

// encodeBlockRecordsAtLevelV10 encodes using V10 contiguous key format (for reference/testing)
// V10 Layout: [keys×N×keySize][wins×N×2][draws×N×2][losses×N×2][cp×N×2][dtm×N×2][dtz×N×2][provenDepth×N×2]
func encodeBlockRecordsAtLevelV10(records []BlockRecord, level int) []byte {
	n := len(records)
	if n == 0 {
		return nil
	}

	keySize := BlockKeySizeAtLevel(level)
	recordSize := keySize + positionRecordSize
	data := make([]byte, n*recordSize)

	// V10: Contiguous keys
	offKeys := 0
	offWins := n * keySize
	offDraws := offWins + n*2
	offLosses := offDraws + n*2
	offCP := offLosses + n*2
	offDTM := offCP + n*2
	offDTZ := offDTM + n*2
	offProvenDepth := offDTZ + n*2

	for i, rec := range records {
		// Write key bytes contiguously
		copy(data[offKeys+i*keySize:offKeys+(i+1)*keySize], rec.Key[:keySize])

		// Data columns
		binary.BigEndian.PutUint16(data[offWins+i*2:], rec.Data.Wins)
		binary.BigEndian.PutUint16(data[offDraws+i*2:], rec.Data.Draws)
		binary.BigEndian.PutUint16(data[offLosses+i*2:], rec.Data.Losses)
		binary.BigEndian.PutUint16(data[offCP+i*2:], uint16(rec.Data.CP))
		binary.BigEndian.PutUint16(data[offDTM+i*2:], uint16(rec.Data.DTM))
		binary.BigEndian.PutUint16(data[offDTZ+i*2:], rec.Data.DTZ)
		binary.BigEndian.PutUint16(data[offProvenDepth+i*2:], rec.Data.ProvenDepth)
	}

	return data
}

// decodeBlockRecordsAtLevel decodes block records based on format version
func decodeBlockRecordsAtLevel(data []byte, n int, level int, version uint8) ([]BlockRecord, error) {
	if version >= PSBlockVersionV11 {
		return decodeBlockRecordsV11(data, n, level)
	}
	return decodeBlockRecordsV10(data, n, level)
}

// decodeBlockRecordsV11 decodes V11 striped key format
func decodeBlockRecordsV11(data []byte, n int, level int) ([]BlockRecord, error) {
	keySize := BlockKeySizeAtLevel(level)
	recordSize := keySize + positionRecordSize
	expectedSize := n * recordSize
	if len(data) < expectedSize {
		return nil, fmt.Errorf("data too short: got %d, need %d for %d records at level %d", len(data), expectedSize, n, level)
	}

	records := make([]BlockRecord, n)

	// V11: Keys striped by byte position
	for b := 0; b < keySize; b++ {
		off := b * n // Offset for this byte position
		for i := 0; i < n; i++ {
			records[i].Key[b] = data[off+i]
		}
	}

	// Data columns (same as V10)
	offData := n * keySize
	offWins := offData
	offDraws := offWins + n*2
	offLosses := offDraws + n*2
	offCP := offLosses + n*2
	offDTM := offCP + n*2
	offDTZ := offDTM + n*2
	offProvenDepth := offDTZ + n*2

	for i := 0; i < n; i++ {
		records[i].Data.Wins = binary.BigEndian.Uint16(data[offWins+i*2:])
		records[i].Data.Draws = binary.BigEndian.Uint16(data[offDraws+i*2:])
		records[i].Data.Losses = binary.BigEndian.Uint16(data[offLosses+i*2:])
		records[i].Data.CP = int16(binary.BigEndian.Uint16(data[offCP+i*2:]))
		records[i].Data.DTM = int16(binary.BigEndian.Uint16(data[offDTM+i*2:]))
		records[i].Data.DTZ = binary.BigEndian.Uint16(data[offDTZ+i*2:])
		records[i].Data.ProvenDepth = binary.BigEndian.Uint16(data[offProvenDepth+i*2:])
	}

	return records, nil
}

// decodeBlockRecordsV10 decodes V10 contiguous key format
func decodeBlockRecordsV10(data []byte, n int, level int) ([]BlockRecord, error) {
	keySize := BlockKeySizeAtLevel(level)
	recordSize := keySize + positionRecordSize
	expectedSize := n * recordSize
	if len(data) < expectedSize {
		return nil, fmt.Errorf("data too short: got %d, need %d for %d records at level %d", len(data), expectedSize, n, level)
	}

	records := make([]BlockRecord, n)

	// V10: Contiguous keys
	offKeys := 0
	offWins := n * keySize
	offDraws := offWins + n*2
	offLosses := offDraws + n*2
	offCP := offLosses + n*2
	offDTM := offCP + n*2
	offDTZ := offDTM + n*2
	offProvenDepth := offDTZ + n*2

	for i := 0; i < n; i++ {
		// Read key bytes contiguously
		copy(records[i].Key[:keySize], data[offKeys+i*keySize:offKeys+(i+1)*keySize])

		// Data columns
		records[i].Data.Wins = binary.BigEndian.Uint16(data[offWins+i*2:])
		records[i].Data.Draws = binary.BigEndian.Uint16(data[offDraws+i*2:])
		records[i].Data.Losses = binary.BigEndian.Uint16(data[offLosses+i*2:])
		records[i].Data.CP = int16(binary.BigEndian.Uint16(data[offCP+i*2:]))
		records[i].Data.DTM = int16(binary.BigEndian.Uint16(data[offDTM+i*2:]))
		records[i].Data.DTZ = binary.BigEndian.Uint16(data[offDTZ+i*2:])
		records[i].Data.ProvenDepth = binary.BigEndian.Uint16(data[offProvenDepth+i*2:])
	}

	return records, nil
}

// BlockHeader is the header for block files (V9)
// 17 bytes: [Magic:4][Version:1][Flags:1][Level:1][RecordCount:4][Checksum:4][Reserved:2]
type BlockHeader struct {
	Magic       [4]byte
	Version     uint8
	Flags       uint8
	Level       uint8 // Block level (1-7)
	RecordCount uint32
	Checksum    uint32
	Reserved    uint16
}

func encodeBlockHeader(h BlockHeader) []byte {
	buf := make([]byte, PSBlockHeaderSize)
	copy(buf[0:4], h.Magic[:])
	buf[4] = h.Version
	buf[5] = h.Flags
	buf[6] = h.Level
	binary.BigEndian.PutUint32(buf[7:11], h.RecordCount)
	binary.BigEndian.PutUint32(buf[11:15], h.Checksum)
	binary.BigEndian.PutUint16(buf[15:17], h.Reserved)
	return buf
}

func decodeBlockHeader(data []byte) (BlockHeader, error) {
	if len(data) < PSBlockHeaderSize {
		return BlockHeader{}, fmt.Errorf("pack header too short: %d bytes", len(data))
	}
	var h BlockHeader
	copy(h.Magic[:], data[0:4])
	h.Version = data[4]
	h.Flags = data[5]
	h.Level = data[6]
	h.RecordCount = binary.BigEndian.Uint32(data[7:11])
	h.Checksum = binary.BigEndian.Uint32(data[11:15])
	h.Reserved = binary.BigEndian.Uint16(data[15:17])
	// Note: level 0 is valid for V9+ _rest.block files
	// Caller should handle version-based defaulting in loadBlockFile
	return h, nil
}

// writeBlockFileAtLevel writes a block file to disk at a specific level
func (ps *PositionStore) writeBlockFileAtLevel(path string, records []BlockRecord, level int) (int64, error) {
	fullPath := filepath.Join(ps.dir, path)

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return 0, fmt.Errorf("create directory: %w", err)
	}

	// Count positions for metadata
	var totalPositions, evalPositions, cpPositions, dtmPositions, dtzPositions uint64
	for _, rec := range records {
		totalPositions++
		hasCP := rec.Data.HasCP()
		hasDTM := rec.Data.DTM != DTMUnknown
		hasDTZ := rec.Data.DTZ != 0
		if hasCP {
			cpPositions++
		}
		if hasDTM {
			dtmPositions++
		}
		if hasDTZ {
			dtzPositions++
		}
		if hasCP || hasDTM || hasDTZ {
			evalPositions++
		}
	}

	// Try to read old file for delta calculation
	var oldTotal, oldEval, oldCP, oldDTM, oldDTZ uint64
	var oldCompressedSize, oldUncompressedSize uint64
	var oldLevel int = level
	isNewFile := true
	if oldData, err := os.ReadFile(fullPath); err == nil {
		isNewFile = false
		oldCompressedSize = uint64(len(oldData))
		if decompressed, err := ps.decoder.DecodeAll(oldData, nil); err == nil {
			oldUncompressedSize = uint64(len(decompressed))
			if len(decompressed) >= PSBlockHeaderSize && string(decompressed[0:4]) == PSBlockMagic {
				if header, err := decodeBlockHeader(decompressed); err == nil {
					oldTotal = uint64(header.RecordCount)
					oldLevel = int(header.Level)
					// Decode old records to count their stats
					if oldRecords, err := decodeBlockRecordsAtLevel(decompressed[PSBlockHeaderSize:], int(header.RecordCount), oldLevel, header.Version); err == nil {
						for _, rec := range oldRecords {
							if rec.Data.HasCP() {
								oldCP++
							}
							if rec.Data.DTM != DTMUnknown {
								oldDTM++
							}
							if rec.Data.DTZ != 0 {
								oldDTZ++
							}
							if rec.Data.HasCP() || rec.Data.DTM != DTMUnknown || rec.Data.DTZ != 0 {
								oldEval++
							}
						}
					}
				}
			}
		}
	}

	// Encode records in columnar format for this level
	recordData := encodeBlockRecordsAtLevel(records, level)

	// Build header
	header := BlockHeader{
		Version:     PSBlockVersion,
		Flags:       0,
		Level:       uint8(level),
		RecordCount: uint32(len(records)),
		Checksum:    crc32.ChecksumIEEE(recordData),
		Reserved:    0,
	}
	copy(header.Magic[:], PSBlockMagic)

	// Combine header + records
	data := make([]byte, PSBlockHeaderSize+len(recordData))
	copy(data[:PSBlockHeaderSize], encodeBlockHeader(header))
	copy(data[PSBlockHeaderSize:], recordData)

	// Compress
	compressed := ps.encoder.EncodeAll(data, nil)
	compressedSize := uint64(len(compressed))
	uncompressedSize := uint64(len(data))

	// Update global metadata with delta
	ps.metaMu.Lock()
	ps.meta.TotalPositions = ps.meta.TotalPositions - oldTotal + totalPositions
	ps.meta.EvaluatedPositions = ps.meta.EvaluatedPositions - oldEval + evalPositions
	ps.meta.CPPositions = ps.meta.CPPositions - oldCP + cpPositions
	ps.meta.DTMPositions = ps.meta.DTMPositions - oldDTM + dtmPositions
	ps.meta.DTZPositions = ps.meta.DTZPositions - oldDTZ + dtzPositions
	ps.meta.CompressedBytes = ps.meta.CompressedBytes - oldCompressedSize + compressedSize
	ps.meta.UncompressedBytes = ps.meta.UncompressedBytes - oldUncompressedSize + uncompressedSize
	if isNewFile {
		ps.meta.TotalBlocks++
	}
	ps.metaMu.Unlock()

	// Write to disk
	ps.fileMu.Lock()
	err := os.WriteFile(fullPath, compressed, 0644)
	ps.fileMu.Unlock()

	if err != nil {
		return 0, err
	}
	return int64(len(compressed)), nil
}

// subtractBlockMetadata reads a block file and subtracts its metadata counts from global metadata.
// This is used when splitting a block - the parent's counts must be removed before children add them.
func (ps *PositionStore) subtractBlockMetadata(fullPath string) {
	ps.fileMu.RLock()
	oldData, err := os.ReadFile(fullPath)
	ps.fileMu.RUnlock()
	if err != nil {
		// File doesn't exist - nothing to subtract
		return
	}

	oldCompressedSize := uint64(len(oldData))

	decompressed, err := ps.decoder.DecodeAll(oldData, nil)
	if err != nil {
		return
	}
	oldUncompressedSize := uint64(len(decompressed))

	if len(decompressed) < PSBlockHeaderSize || string(decompressed[0:4]) != PSBlockMagic {
		return
	}

	header, err := decodeBlockHeader(decompressed)
	if err != nil {
		return
	}

	oldLevel := int(header.Level)
	oldRecords, err := decodeBlockRecordsAtLevel(decompressed[PSBlockHeaderSize:], int(header.RecordCount), oldLevel, header.Version)
	if err != nil {
		return
	}

	// Count stats from old records
	var oldTotal, oldEval, oldCP, oldDTM, oldDTZ uint64
	oldTotal = uint64(len(oldRecords))
	for _, rec := range oldRecords {
		hasCP := rec.Data.HasCP()
		hasDTM := rec.Data.DTM != DTMUnknown
		hasDTZ := rec.Data.DTZ != 0
		if hasCP {
			oldCP++
		}
		if hasDTM {
			oldDTM++
		}
		if hasDTZ {
			oldDTZ++
		}
		if hasCP || hasDTM || hasDTZ {
			oldEval++
		}
	}

	// Subtract from global metadata
	ps.metaMu.Lock()
	ps.meta.TotalPositions -= oldTotal
	ps.meta.EvaluatedPositions -= oldEval
	ps.meta.CPPositions -= oldCP
	ps.meta.DTMPositions -= oldDTM
	ps.meta.DTZPositions -= oldDTZ
	ps.meta.CompressedBytes -= oldCompressedSize
	ps.meta.UncompressedBytes -= oldUncompressedSize
	ps.meta.TotalBlocks-- // Parent block is being removed
	ps.metaMu.Unlock()
}

// loadBlockFile loads a pack file from disk
func (ps *PositionStore) loadBlockFile(filename string) ([]BlockRecord, error) {
	fullPath := filepath.Join(ps.dir, filename)

	ps.fileMu.RLock()
	compressed, err := os.ReadFile(fullPath)
	ps.fileMu.RUnlock()

	if err != nil {
		return nil, err
	}

	// Decompress
	data, err := ps.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("decompress pack file: %w", err)
	}

	// Check magic
	if len(data) < PSBlockHeaderSize || string(data[0:4]) != PSBlockMagic {
		return nil, fmt.Errorf("invalid pack file magic")
	}

	// Decode header
	header, err := decodeBlockHeader(data)
	if err != nil {
		return nil, fmt.Errorf("decode pack header: %w", err)
	}

	// Verify checksum
	recordData := data[PSBlockHeaderSize:]
	checksum := crc32.ChecksumIEEE(recordData)
	if checksum != header.Checksum {
		ps.log("warning: checksum mismatch in %s (got %x, expected %x)", filename, checksum, header.Checksum)
	}

	// Decode records using level and version from header
	level := int(header.Level)
	records, err := decodeBlockRecordsAtLevel(recordData, int(header.RecordCount), level, header.Version)
	if err != nil {
		return nil, fmt.Errorf("decode pack records: %w", err)
	}

	return records, nil
}

// FlushAll writes all dirty records to disk using parallel workers.
// If a background flush is already running, it waits for that to complete first.
func (ps *PositionStore) FlushAll() error {
	// Wait for any background flush to finish
	ps.WaitForFlush()

	// Now run our flush
	ps.flushMu.Lock()
	if ps.flushRunning {
		// Shouldn't happen after WaitForFlush, but just in case
		ps.flushMu.Unlock()
		ps.WaitForFlush()
		return ps.FlushAll()
	}
	ps.flushRunning = true
	ps.flushMu.Unlock()

	// Loop until all dirty blocks are flushed.
	// Redistribution during flush can create new dirty blocks that need another pass.
	maxPasses := 10 // safety limit to prevent infinite loops
	for pass := 0; pass < maxPasses; pass++ {
		err := ps.flushAllInternal(true)
		if err != nil {
			ps.flushMu.Lock()
			ps.flushRunning = false
			ps.flushMu.Unlock()
			return err
		}

		// Check if redistribution created new dirty blocks
		if ps.DirtyCount() == 0 {
			break
		}
		ps.log("redistribution created new dirty blocks, running additional flush pass %d...", pass+2)
	}

	ps.flushMu.Lock()
	ps.flushRunning = false
	ps.flushMu.Unlock()

	// Flush pending evals to CSV log
	if err := ps.flushEvalLog(); err != nil {
		ps.log("warning: eval log flush failed: %v", err)
	}

	// Small root blocks are fine; they'll be read individually.

	// Refresh metadata from disk to fix any accumulated tracking errors
	if rerr := ps.RefreshMeta(); rerr != nil {
		ps.log("warning: metadata refresh failed: %v", rerr)
	}

	return nil
}

// FlushAllAsync starts a background flush if one isn't already running.
// Returns immediately. Use WaitForFlush to wait for completion.
func (ps *PositionStore) FlushAllAsync() {
	ps.flushMu.Lock()
	if ps.flushRunning {
		ps.flushMu.Unlock()
		return // Already flushing
	}
	ps.flushRunning = true
	ps.flushMu.Unlock()

	go func() {
		ps.flushAllInternal(false)
		ps.flushMu.Lock()
		ps.flushRunning = false
		ps.flushMu.Unlock()
	}()
}

// IsFlushRunning returns true if a background flush is in progress
func (ps *PositionStore) IsFlushRunning() bool {
	ps.flushMu.Lock()
	defer ps.flushMu.Unlock()
	return ps.flushRunning
}

// WaitForFlush waits for any background flush to complete
func (ps *PositionStore) WaitForFlush() {
	for ps.IsFlushRunning() {
		time.Sleep(100 * time.Millisecond)
	}
}

// flushEvalLog writes pending eval entries to the CSV log file
func (ps *PositionStore) flushEvalLog() error {
	if ps.evalLogPath == "" {
		return nil
	}

	// Atomically swap out pending evals
	ps.pendingEvalsMu.Lock()
	evals := ps.pendingEvals
	ps.pendingEvals = nil
	ps.pendingEvalsMu.Unlock()

	if len(evals) == 0 {
		return nil
	}

	// Open file in append mode
	f, err := os.OpenFile(ps.evalLogPath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("open eval log: %w", err)
	}
	defer f.Close()

	// Write each eval entry as a CSV row
	for _, e := range evals {
		// Format: fen,position,cp,dtm,dtz,proven_depth
		fen := e.pos.ToFEN()
		posKey := e.pos.String()
		line := fmt.Sprintf("%s,%s,%d,%d,%d,%d\n",
			fen, posKey, e.record.CP, e.record.DTM, e.record.DTZ, e.record.ProvenDepth)
		if _, err := f.WriteString(line); err != nil {
			return fmt.Errorf("write eval log: %w", err)
		}
	}

	ps.log("flushed %d evals to %s", len(evals), ps.evalLogPath)
	return nil
}

func (ps *PositionStore) flushAllInternal(blocking bool) error {
	// Collect filenames with their sizes for sorting
	type fileWithSize struct {
		filename      string
		size          int
		estimatedSize int64 // estimated uncompressed bytes
	}

	ps.dirtyMu.Lock()
	files := make([]fileWithSize, 0, len(ps.dirtyBlocks))
	for filename, db := range ps.dirtyBlocks {
		// Calculate estimated size based on record count and level
		_, level, _ := ParseBlockFilePath(filename)
		recordSize := BlockRecordSizeAtLevel(level)
		estimatedSize := int64(len(db.records)*recordSize + PSBlockHeaderSize)
		files = append(files, fileWithSize{filename: filename, size: len(db.records), estimatedSize: estimatedSize})
	}
	ps.dirtyMu.Unlock()

	// Sort by size descending - largest blocks first so they start processing early
	sort.Slice(files, func(i, j int) bool {
		return files[i].size > files[j].size
	})

	dirtyFilenames := make([]string, len(files))
	var totalEstimatedBytes int64
	for i, f := range files {
		dirtyFilenames[i] = f.filename
		totalEstimatedBytes += f.estimatedSize
	}

	if len(dirtyFilenames) == 0 {
		return nil
	}

	numWorkers := runtime.NumCPU()
	if numWorkers > len(dirtyFilenames) {
		numWorkers = len(dirtyFilenames)
	}
	if numWorkers > 16 {
		numWorkers = 16 // cap at 16 to avoid too many concurrent disk writes
	}

	total := len(dirtyFilenames)
	ps.log("flushing %d dirty block files with %d workers...", total, numWorkers)
	start := time.Now()

	// Progress tracking
	var flushed int64
	var compressedWritten int64
	var uncompressedWritten int64

	// Create work channel and error channel
	filenameChan := make(chan string, len(dirtyFilenames))
	errChan := make(chan error, numWorkers)

	// Progress logger goroutine
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				f := atomic.LoadInt64(&flushed)
				compressed := atomic.LoadInt64(&compressedWritten)
				uncompressed := atomic.LoadInt64(&uncompressedWritten)
				elapsed := time.Since(start)
				rate := float64(f) / elapsed.Seconds()
				uncompressedPct := float64(0)
				if totalEstimatedBytes > 0 {
					uncompressedPct = float64(uncompressed) / float64(totalEstimatedBytes) * 100
				}
				ps.log("flush progress: %d/%d blocks (%.1f%%), %.1f/%.1f MB uncompressed (%.1f%%), %.1f MB compressed, %.1f blocks/sec",
					f, total, float64(f)/float64(total)*100,
					float64(uncompressed)/(1024*1024), float64(totalEstimatedBytes)/(1024*1024), uncompressedPct,
					float64(compressed)/(1024*1024),
					rate)
			}
		}
	}()

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for filename := range filenameChan {
				compressed, uncompressed, err := ps.flushBlockFileWithSize(filename)
				if err != nil {
					select {
					case errChan <- fmt.Errorf("flush %s: %w", filename, err):
					default:
					}
					return
				}
				atomic.AddInt64(&flushed, 1)
				atomic.AddInt64(&compressedWritten, compressed)
				atomic.AddInt64(&uncompressedWritten, uncompressed)
			}
		}()
	}

	// Send work
	for _, filename := range dirtyFilenames {
		filenameChan <- filename
	}
	close(filenameChan)

	// Wait for workers
	wg.Wait()
	close(errChan)
	close(done)

	// Check for errors
	if err := <-errChan; err != nil {
		return err
	}

	elapsed := time.Since(start)
	ps.log("flush complete: %d blocks, %.1f MB uncompressed, %.1f MB compressed in %v (%.1f blocks/sec)",
		total, float64(uncompressedWritten)/(1024*1024), float64(compressedWritten)/(1024*1024), elapsed, float64(total)/elapsed.Seconds())
	return nil
}

// DirtyCount returns the number of block files with pending writes
func (ps *PositionStore) DirtyCount() int {
	ps.dirtyMu.Lock()
	count := len(ps.dirtyBlocks)
	ps.dirtyMu.Unlock()
	return count
}

// FlushIfNeeded flushes if dirty count exceeds threshold
func (ps *PositionStore) FlushIfNeeded(threshold int) error {
	if ps.DirtyCount() >= threshold {
		return ps.FlushAll()
	}
	return nil
}

// FlushIfNeededAsync starts a background flush if dirty count exceeds threshold.
// Returns true if a flush was started or is already running.
func (ps *PositionStore) FlushIfNeededAsync(threshold int) bool {
	if ps.DirtyCount() >= threshold {
		if ps.IsFlushRunning() {
			return true // Already flushing
		}
		ps.FlushAllAsync()
		return true
	}
	return false
}

// DirtyBytes returns the approximate memory used by dirty blocks in bytes
func (ps *PositionStore) DirtyBytes() int64 {
	ps.dirtyMu.Lock()
	bytes := ps.dirtyBytes
	ps.dirtyMu.Unlock()
	return bytes
}

// FlushIfMemoryNeeded flushes if dirty memory exceeds threshold bytes
func (ps *PositionStore) FlushIfMemoryNeeded(thresholdBytes int64) error {
	if ps.DirtyBytes() >= thresholdBytes {
		return ps.FlushAll()
	}
	return nil
}

// FlushIfMemoryNeededAsync starts a background flush if dirty memory exceeds threshold.
// Returns true if a flush was started or is already running.
func (ps *PositionStore) FlushIfMemoryNeededAsync(thresholdBytes int64) bool {
	if ps.DirtyBytes() >= thresholdBytes {
		if ps.IsFlushRunning() {
			return true // Already flushing
		}
		ps.FlushAllAsync()
		return true
	}
	return false
}

// IncrementGameCount increments the total games counter by n
func (ps *PositionStore) IncrementGameCount(n uint64) {
	ps.metaMu.Lock()
	ps.meta.TotalGames += n
	ps.metaMu.Unlock()
}

// flushBlockFileWithSize flushes a single pack file and returns (compressed, uncompressed) bytes written
func (ps *PositionStore) flushBlockFileWithSize(filename string) (int64, int64, error) {
	ps.dirtyMu.Lock()
	db, ok := ps.dirtyBlocks[filename]
	if !ok || len(db.records) == 0 {
		ps.dirtyMu.Unlock()
		return 0, 0, nil
	}
	// Compact to merge duplicates and sort
	db.compact()
	// Move to inflight so Get() can still find these records during flush
	newRecords := db.records
	level := db.level // Save level before deleting
	// Calculate uncompressed size from record count and level
	uncompressedSize := int64(len(newRecords)*BlockRecordSizeAtLevel(level) + PSBlockHeaderSize)
	ps.inflightBlocks[filename] = newRecords
	// Subtract memory for flushed records
	ps.dirtyBytes -= int64(len(newRecords)) * DirtyEntrySize
	ps.dirtyBytes -= int64(len(filename)) + 50 // outer map entry overhead
	if ps.dirtyBytes < 0 {
		ps.dirtyBytes = 0 // safety clamp
	}
	delete(ps.dirtyBlocks, filename)
	ps.dirtyMu.Unlock()

	// Ensure we clean up inflight on exit (success or failure)
	defer func() {
		ps.dirtyMu.Lock()
		delete(ps.inflightBlocks, filename)
		ps.dirtyMu.Unlock()
	}()

	// Invalidate cache
	ps.invalidateBlockCache(filename)

	// Check if the path has been split (directory exists where file should be)
	// This can happen if another goroutine split the block between Put() and flush
	dirPath := strings.TrimSuffix(filepath.Join(ps.dir, filename), ".block")
	if info, err := os.Stat(dirPath); err == nil && info.IsDir() {
		// Block was split! Redistribute records to correct child paths
		ps.log("flush detected split: redistributing %d records from %s to children", len(newRecords), filename)
		compressed, err := ps.redistributeToChildren(newRecords, level, filename)
		return compressed, uncompressedSize, err
	}

	// Try to load existing records
	var existingRecords []BlockRecord
	existing, err := ps.loadBlockFile(filename)
	if err == nil {
		existingRecords = existing
	} else if !os.IsNotExist(err) {
		return 0, 0, fmt.Errorf("load existing: %w", err)
	}

	// newRecords already sorted from compact()
	// Merge (use key size for proper comparison)
	keySize := BlockKeySizeAtLevel(level)

	// Count evals before merge for debugging
	var existingEvals, newEvals int
	for _, rec := range existingRecords {
		if rec.Data.HasCP() || rec.Data.DTM != DTMUnknown || rec.Data.DTZ != 0 {
			existingEvals++
		}
	}
	for _, rec := range newRecords {
		if rec.Data.HasCP() || rec.Data.DTM != DTMUnknown || rec.Data.DTZ != 0 {
			newEvals++
		}
	}

	merged := mergeBlockRecords(existingRecords, newRecords, keySize)

	// Count evals after merge
	var mergedEvals int
	for _, rec := range merged {
		if rec.Data.HasCP() || rec.Data.DTM != DTMUnknown || rec.Data.DTZ != 0 {
			mergedEvals++
		}
	}

	// Log if evals were lost during merge
	if existingEvals > 0 && mergedEvals < existingEvals {
		ps.log("WARNING: evals lost in flush merge for %s: existing=%d, new=%d, merged=%d",
			filename, existingEvals, newEvals, mergedEvals)
	}

	// Check if block needs to be split BEFORE writing (in-memory splitting)
	// This avoids write-read-split-rewrite cycle
	recordSize := BlockRecordSizeAtLevel(level)
	estimatedSize := uint64(len(merged)*recordSize + PSBlockHeaderSize)
	if estimatedSize > ps.maxBlockSizeBytes && level < MaxBlockLevel {
		// Split in memory and write directly to child blocks
		compressed, err := ps.splitAndFlushInMemory(filename, merged, level)
		return compressed, uncompressedSize, err
	}

	// Block is under threshold, write directly
	written, err := ps.writeBlockFileAtLevel(filename, merged, level)
	if err != nil {
		return 0, 0, err
	}

	// Update cache
	ps.addToBlockCache(filename, &psBlockCache{
		filename: filename,
		records:  merged,
		level:    level,
	})

	return written, uncompressedSize, nil
}

// splitAndFlushInMemory partitions records by first key byte and writes directly to child blocks.
// V10: Unified splitting - all levels split by first byte, shifting 1 byte at a time.
func (ps *PositionStore) splitAndFlushInMemory(filename string, records []BlockRecord, level int) (int64, error) {
	if level >= MaxBlockLevel {
		// Can't split at max level, just write
		return ps.writeBlockFileAtLevel(filename, records, level)
	}

	keySize := BlockKeySizeAtLevel(level)
	childLevel := level + 1
	childKeySize := BlockKeySizeAtLevel(childLevel)

	// V10: Unified - always shift 1 byte per level
	const shiftBytes = 1

	// Parent directory (path without .block extension)
	parentDir := strings.TrimSuffix(filename, ".block")

	// Count evals in records being split
	var evalCount int
	for _, rec := range records {
		if rec.Data.HasCP() || rec.Data.DTM != DTMUnknown || rec.Data.DTZ != 0 {
			evalCount++
		}
	}
	ps.log("in-memory split: %s (level %d, %d records, %d evals) -> children at level %d",
		filename, level, len(records), evalCount, childLevel)

	// CRITICAL: Before deleting parent file, subtract its metadata counts.
	// The parent's positions are already counted in global metadata.
	// Children will add them as new (since no old child files exist).
	// Without this subtraction, positions get double-counted.
	parentFile := filepath.Join(ps.dir, filename)
	ps.subtractBlockMetadata(parentFile)

	// Delete parent file - data is in memory, children will be written
	if err := os.Remove(parentFile); err != nil && !os.IsNotExist(err) {
		ps.log("warning: failed to remove parent before split: %v", err)
	}

	// CRITICAL: Invalidate path cache since file structure changed.
	// Without this, cached paths point to the old parent file which no longer exists,
	// causing reads to fail and data to appear "lost".
	ps.InvalidatePathCache()

	// V10: All levels use the same uniform split logic - group by first byte
	return ps.splitByFirstByteInMemory(parentDir, records, keySize, childKeySize, shiftBytes, childLevel)
}

// splitByFirstByteInMemory is the V10 unified split function.
// All levels split by first key byte, creating 256 possible children.
func (ps *PositionStore) splitByFirstByteInMemory(parentDir string, records []BlockRecord, keySize, childKeySize, shiftBytes, childLevel int) (int64, error) {
	// Group by first byte
	byteGroups := make([][]BlockRecord, 256)
	for i := range byteGroups {
		byteGroups[i] = make([]BlockRecord, 0)
	}
	for _, rec := range records {
		firstByte := rec.Key[0]
		byteGroups[firstByte] = append(byteGroups[firstByte], rec)
	}

	// Create parent directory
	fullParentDir := filepath.Join(ps.dir, parentDir)
	if err := os.MkdirAll(fullParentDir, 0755); err != nil {
		return 0, fmt.Errorf("create split dir: %w", err)
	}

	var totalWritten int64

	// Write individual files for each byte value
	for byteVal := 0; byteVal < 256; byteVal++ {
		if len(byteGroups[byteVal]) == 0 {
			continue
		}

		childName := fmt.Sprintf("%02x", byteVal)
		childPath := filepath.Join(parentDir, childName+".block")

		// Create shifted records (shift 1 byte)
		childRecords := make([]BlockRecord, len(byteGroups[byteVal]))
		for i, rec := range byteGroups[byteVal] {
			childRecords[i] = BlockRecord{Data: rec.Data}
			copy(childRecords[i].Key[:], rec.Key[shiftBytes:keySize])
		}

		// Sort for merge
		sort.Slice(childRecords, func(i, j int) bool {
			return bytes.Compare(childRecords[i].Key[:childKeySize], childRecords[j].Key[:childKeySize]) < 0
		})

		// Recursively flush (may split further if still too large)
		written, err := ps.flushRecordsToPath(childPath, childRecords, childLevel)
		if err != nil {
			return totalWritten, fmt.Errorf("flush child %s: %w", childPath, err)
		}
		totalWritten += written
	}

	return totalWritten, nil
}

// flushRecordsToPath writes records to a path, merging with existing data and splitting if needed
func (ps *PositionStore) flushRecordsToPath(filename string, newRecords []BlockRecord, level int) (int64, error) {
	// Invalidate cache
	ps.invalidateBlockCache(filename)

	// Check if the path has been split (directory exists where file should be)
	dirPath := strings.TrimSuffix(filepath.Join(ps.dir, filename), ".block")
	if info, err := os.Stat(dirPath); err == nil && info.IsDir() {
		// Already split, redistribute to children
		return ps.redistributeToChildren(newRecords, level, filename)
	}

	// Load existing records
	var existingRecords []BlockRecord
	existing, err := ps.loadBlockFile(filename)
	if err == nil {
		existingRecords = existing
	} else if !os.IsNotExist(err) {
		return 0, fmt.Errorf("load existing: %w", err)
	}

	// Merge
	keySize := BlockKeySizeAtLevel(level)
	merged := mergeBlockRecords(existingRecords, newRecords, keySize)

	// Check if we need to split
	recordSize := BlockRecordSizeAtLevel(level)
	estimatedSize := uint64(len(merged)*recordSize + PSBlockHeaderSize)
	if estimatedSize > ps.maxBlockSizeBytes && level < MaxBlockLevel {
		// Recursively split in memory
		return ps.splitAndFlushInMemory(filename, merged, level)
	}

	// Write directly
	written, err := ps.writeBlockFileAtLevel(filename, merged, level)
	if err != nil {
		return 0, err
	}

	// Update cache
	ps.addToBlockCache(filename, &psBlockCache{
		filename: filename,
		records:  merged,
		level:    level,
	})

	return written, nil
}

// redistributeToChildren handles the case where a dirty block's target path
// was split by another goroutine. It redistributes the records to the correct
// child paths based on the new level structure.
// V10: Unified logic - always shift 1 byte per level
func (ps *PositionStore) redistributeToChildren(records []BlockRecord, level int, oldPath string) (int64, error) {
	if level >= MaxBlockLevel {
		// Can't redistribute at max level
		ps.log("warning: cannot redistribute at max level %d for %s", level, oldPath)
		return 0, nil
	}

	childLevel := level + 1

	// V10: Always shift 1 byte
	const shiftBytes = 1

	// Group records by child path
	childRecords := make(map[string][]BlockRecord)
	parentDir := strings.TrimSuffix(oldPath, ".block")

	for _, rec := range records {
		// V10: All levels use uniform 2-hex-char naming based on first byte
		firstByte := rec.Key[0]
		childName := fmt.Sprintf("%02x", firstByte)
		childPath := filepath.Join(parentDir, childName+".block")

		// Create child record with shifted key
		childRec := BlockRecord{Data: rec.Data}
		copy(childRec.Key[:], rec.Key[shiftBytes:])

		childRecords[childPath] = append(childRecords[childPath], childRec)
	}

	// Put records into dirty blocks for each child path
	ps.dirtyMu.Lock()
	for childPath, recs := range childRecords {
		db := ps.dirtyBlocks[childPath]
		if db == nil {
			db = &dirtyBlock{records: make([]BlockRecord, 0, len(recs)), level: childLevel}
			ps.dirtyBlocks[childPath] = db
			ps.dirtyBytes += int64(len(childPath)) + 50
		}
		for _, rec := range recs {
			db.append(rec)
			ps.dirtyBytes += DirtyEntrySize
		}
	}
	ps.dirtyMu.Unlock()

	ps.log("redistributed %d records from %s to %d child paths at level %d",
		len(records), oldPath, len(childRecords), childLevel)

	return 0, nil // No bytes written directly; records will be flushed later
}

// mergeBlockRecords merges two sorted pack record slices
// keySize determines how many bytes of the key to compare
func mergeBlockRecords(existing, newer []BlockRecord, keySize int) []BlockRecord {
	if len(existing) == 0 {
		return newer
	}
	if len(newer) == 0 {
		return existing
	}

	result := make([]BlockRecord, 0, len(existing)+len(newer))
	i, j := 0, 0

	for i < len(existing) && j < len(newer) {
		cmp := bytes.Compare(existing[i].Key[:keySize], newer[j].Key[:keySize])
		if cmp < 0 {
			result = append(result, existing[i])
			i++
		} else if cmp > 0 {
			result = append(result, newer[j])
			j++
		} else {
			// Same key: merge by adding counts and keeping best eval data
			merged := existing[i].Data
			// Skip count updates if any field is already saturated (preserves ratios)
			if merged.Wins < 65535 && merged.Draws < 65535 && merged.Losses < 65535 {
				newWins := uint32(merged.Wins) + uint32(newer[j].Data.Wins)
				if newWins > 65535 {
					newWins = 65535
				}
				newDraws := uint32(merged.Draws) + uint32(newer[j].Data.Draws)
				if newDraws > 65535 {
					newDraws = 65535
				}
				newLosses := uint32(merged.Losses) + uint32(newer[j].Data.Losses)
				if newLosses > 65535 {
					newLosses = 65535
				}
				merged.Wins = uint16(newWins)
				merged.Draws = uint16(newDraws)
				merged.Losses = uint16(newLosses)
			}
			// Keep evaluation data from newer if it has any, otherwise keep existing
			if newer[j].Data.HasCP() || newer[j].Data.DTM != DTMUnknown || newer[j].Data.DTZ != 0 {
				merged.CP = newer[j].Data.CP
				merged.DTM = newer[j].Data.DTM
				merged.DTZ = newer[j].Data.DTZ
				merged.ProvenDepth = newer[j].Data.ProvenDepth
			}
			result = append(result, BlockRecord{Key: existing[i].Key, Data: merged})
			i++
			j++
		}
	}

	result = append(result, existing[i:]...)
	result = append(result, newer[j:]...)

	return result
}

// PSStats holds position store statistics
type PSStats struct {
	TotalReads         uint64
	TotalWrites        uint64
	DirtyFiles         int
	DirtyBytes         int64 // Approximate memory used by dirty blocks
	CachedBlocks       int
	TotalPositions     uint64 // Total positions in the database
	EvaluatedPositions uint64 // Positions with any evaluation (CP, DTM, or DTZ)
	CPPositions        uint64 // Positions with centipawn evaluation
	DTMPositions       uint64 // Positions with distance-to-mate
	DTZPositions       uint64 // Positions with distance-to-zeroing
	UncompressedBytes  uint64 // Total uncompressed size of all blocks
	CompressedBytes    uint64 // Total compressed size of all blocks on disk
	TotalGames         uint64 // Total games ingested
	TotalFolders       uint64 // Total folders (king position combinations)
	TotalBlocks        uint64 // Total block files

	// Positions per block distribution
	PositionsPerBlockMin uint64
	PositionsPerBlockP25 uint64
	PositionsPerBlockP50 uint64
	PositionsPerBlockP75 uint64
	PositionsPerBlockMax uint64

	// Block size distribution (compressed bytes)
	BlockSizeBytesMin uint64
	BlockSizeBytesP25 uint64
	BlockSizeBytesP50 uint64
	BlockSizeBytesP75 uint64
	BlockSizeBytesMax uint64

	// Block format version counts
	V10Blocks uint64
	V11Blocks uint64
}

// PSMeta holds persistent metadata for the position store
type PSMeta struct {
	TotalPositions     uint64 `json:"total_positions"`
	EvaluatedPositions uint64 `json:"evaluated_positions"`
	CPPositions        uint64 `json:"cp_positions"`
	DTMPositions       uint64 `json:"dtm_positions"`
	DTZPositions       uint64 `json:"dtz_positions"`
	UncompressedBytes  uint64 `json:"uncompressed_bytes"`
	CompressedBytes    uint64 `json:"compressed_bytes"`
	TotalGames         uint64 `json:"total_games"`
	TotalFolders       uint64 `json:"total_folders"`
	TotalBlocks        uint64 `json:"total_blocks"`

	// Positions per block distribution
	PositionsPerBlockMin uint64 `json:"positions_per_block_min"`
	PositionsPerBlockP25 uint64 `json:"positions_per_block_p25"`
	PositionsPerBlockP50 uint64 `json:"positions_per_block_p50"`
	PositionsPerBlockP75 uint64 `json:"positions_per_block_p75"`
	PositionsPerBlockMax uint64 `json:"positions_per_block_max"`

	// Block size distribution (compressed bytes)
	BlockSizeBytesMin uint64 `json:"block_size_bytes_min"`
	BlockSizeBytesP25 uint64 `json:"block_size_bytes_p25"`
	BlockSizeBytesP50 uint64 `json:"block_size_bytes_p50"`
	BlockSizeBytesP75 uint64 `json:"block_size_bytes_p75"`
	BlockSizeBytesMax uint64 `json:"block_size_bytes_max"`

	// Block format version counts
	V10Blocks uint64 `json:"v10_blocks"`
	V11Blocks uint64 `json:"v11_blocks"`
}

// Stats returns position store statistics
func (ps *PositionStore) Stats() PSStats {
	ps.dirtyMu.Lock()
	dirtyCount := len(ps.dirtyBlocks)
	dirtyBytes := ps.dirtyBytes
	ps.dirtyMu.Unlock()

	ps.cacheMu.RLock()
	cacheCount := ps.cacheList.Len()
	ps.cacheMu.RUnlock()

	ps.metaMu.RLock()
	meta := ps.meta
	ps.metaMu.RUnlock()

	return PSStats{
		TotalReads:         ps.totalReads,
		TotalWrites:        ps.totalWrites,
		DirtyFiles:         dirtyCount,
		DirtyBytes:         dirtyBytes,
		CachedBlocks:       cacheCount,
		TotalPositions:     meta.TotalPositions,
		EvaluatedPositions: meta.EvaluatedPositions,
		CPPositions:        meta.CPPositions,
		DTMPositions:       meta.DTMPositions,
		DTZPositions:       meta.DTZPositions,
		UncompressedBytes:  meta.UncompressedBytes,
		CompressedBytes:    meta.CompressedBytes,
		TotalGames:         meta.TotalGames,
		TotalFolders:       meta.TotalFolders,
		TotalBlocks:        meta.TotalBlocks,

		PositionsPerBlockMin: meta.PositionsPerBlockMin,
		PositionsPerBlockP25: meta.PositionsPerBlockP25,
		PositionsPerBlockP50: meta.PositionsPerBlockP50,
		PositionsPerBlockP75: meta.PositionsPerBlockP75,
		PositionsPerBlockMax: meta.PositionsPerBlockMax,

		BlockSizeBytesMin: meta.BlockSizeBytesMin,
		BlockSizeBytesP25: meta.BlockSizeBytesP25,
		BlockSizeBytesP50: meta.BlockSizeBytesP50,
		BlockSizeBytesP75: meta.BlockSizeBytesP75,
		BlockSizeBytesMax: meta.BlockSizeBytesMax,

		V10Blocks: meta.V10Blocks,
		V11Blocks: meta.V11Blocks,
	}
}

// LoadMeta loads metadata from disk
func (ps *PositionStore) LoadMeta() error {
	metaPath := filepath.Join(ps.dir, PSMetaFileName)
	data, err := os.ReadFile(metaPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No metadata file yet, that's ok
			return nil
		}
		return fmt.Errorf("read metadata: %w", err)
	}

	var meta PSMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("parse metadata: %w", err)
	}

	ps.metaMu.Lock()
	ps.meta = meta
	ps.metaMu.Unlock()

	return nil
}

// SaveMeta saves metadata to disk
func (ps *PositionStore) SaveMeta() error {
	if ps.readOnly {
		return ErrPSReadOnly
	}

	ps.metaMu.RLock()
	meta := ps.meta
	ps.metaMu.RUnlock()

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	metaPath := filepath.Join(ps.dir, PSMetaFileName)
	if err := os.WriteFile(metaPath, data, 0644); err != nil {
		return fmt.Errorf("write metadata: %w", err)
	}

	return nil
}

// blockStats holds statistics for a single block file
type blockStats struct {
	fileSize          uint64
	numRecords        uint64
	uncompressedBytes uint64
	cpPositions       uint64
	dtmPositions      uint64
	dtzPositions      uint64
	evalPositions     uint64
	isV11             bool
}

// RefreshMeta recalculates metadata by scanning all block files on disk.
// Uses parallel workers for faster processing.
func (ps *PositionStore) RefreshMeta() error {
	ps.log("refreshing metadata from disk...")

	// First, collect all block file paths
	var blockPaths []string
	err := filepath.WalkDir(ps.dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(d.Name(), ".block") {
			relPath, _ := filepath.Rel(ps.dir, path)
			blockPaths = append(blockPaths, relPath)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk blocks: %w", err)
	}

	if len(blockPaths) == 0 {
		ps.metaMu.Lock()
		ps.meta = PSMeta{TotalGames: ps.meta.TotalGames}
		ps.metaMu.Unlock()
		ps.log("metadata refreshed: 0 blocks")
		return nil
	}

	// Process blocks in parallel
	numWorkers := runtime.NumCPU()
	if numWorkers > 16 {
		numWorkers = 16
	}

	pathChan := make(chan string, len(blockPaths))
	resultChan := make(chan blockStats, len(blockPaths))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Each worker needs its own decoder for thread safety
			decoder, _ := zstd.NewReader(nil)
			defer decoder.Close()

			for relPath := range pathChan {
				stats := ps.processBlockForRefresh(relPath, decoder)
				resultChan <- stats
			}
		}()
	}

	// Send work
	for _, path := range blockPaths {
		pathChan <- path
	}
	close(pathChan)

	// Wait for workers and close results
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	var meta PSMeta
	meta.TotalGames = ps.meta.TotalGames // Preserve game count

	var blockSizes []uint64
	var positionCounts []uint64

	for stats := range resultChan {
		meta.TotalBlocks++
		meta.CompressedBytes += stats.fileSize
		meta.TotalPositions += stats.numRecords
		meta.UncompressedBytes += stats.uncompressedBytes
		meta.CPPositions += stats.cpPositions
		meta.DTMPositions += stats.dtmPositions
		meta.DTZPositions += stats.dtzPositions
		meta.EvaluatedPositions += stats.evalPositions

		if stats.isV11 {
			meta.V11Blocks++
		} else {
			meta.V10Blocks++
		}

		blockSizes = append(blockSizes, stats.fileSize)
		positionCounts = append(positionCounts, stats.numRecords)
	}

	// Calculate percentiles
	if len(blockSizes) > 0 {
		sort.Slice(blockSizes, func(i, j int) bool { return blockSizes[i] < blockSizes[j] })
		meta.BlockSizeBytesMin = blockSizes[0]
		meta.BlockSizeBytesMax = blockSizes[len(blockSizes)-1]
		meta.BlockSizeBytesP25 = blockSizes[len(blockSizes)*25/100]
		meta.BlockSizeBytesP50 = blockSizes[len(blockSizes)*50/100]
		meta.BlockSizeBytesP75 = blockSizes[len(blockSizes)*75/100]
	}
	if len(positionCounts) > 0 {
		sort.Slice(positionCounts, func(i, j int) bool { return positionCounts[i] < positionCounts[j] })
		meta.PositionsPerBlockMin = positionCounts[0]
		meta.PositionsPerBlockMax = positionCounts[len(positionCounts)-1]
		meta.PositionsPerBlockP25 = positionCounts[len(positionCounts)*25/100]
		meta.PositionsPerBlockP50 = positionCounts[len(positionCounts)*50/100]
		meta.PositionsPerBlockP75 = positionCounts[len(positionCounts)*75/100]
	}

	// Update metadata
	ps.metaMu.Lock()
	ps.meta = meta
	ps.metaMu.Unlock()

	ps.log("metadata refreshed: %d blocks, %d positions, %d evals, %d bytes (v10=%d, v11=%d)",
		meta.TotalBlocks, meta.TotalPositions, meta.EvaluatedPositions, meta.CompressedBytes,
		meta.V10Blocks, meta.V11Blocks)

	return nil
}

// processBlockForRefresh processes a single block file and returns its stats
func (ps *PositionStore) processBlockForRefresh(relPath string, decoder *zstd.Decoder) blockStats {
	var stats blockStats

	fullPath := filepath.Join(ps.dir, relPath)

	// Get file size
	info, err := os.Stat(fullPath)
	if err != nil {
		return stats
	}
	stats.fileSize = uint64(info.Size())

	// Read and decompress
	compressedData, err := os.ReadFile(fullPath)
	if err != nil {
		return stats
	}

	decompressed, err := decoder.DecodeAll(compressedData, nil)
	if err != nil || len(decompressed) < PSBlockHeaderSize {
		return stats
	}

	header, err := decodeBlockHeader(decompressed)
	if err != nil {
		return stats
	}

	stats.isV11 = header.Version >= PSBlockVersionV11

	// Decode records
	_, level, _ := ParseBlockFilePath(relPath)
	records, err := decodeBlockRecordsAtLevel(decompressed[PSBlockHeaderSize:], int(header.RecordCount), level, header.Version)
	if err != nil {
		return stats
	}

	stats.numRecords = uint64(len(records))
	stats.uncompressedBytes = uint64(len(records) * BlockRecordSizeAtLevel(level))

	for _, rec := range records {
		hasCP := rec.Data.HasCP()
		hasDTM := rec.Data.DTM != DTMUnknown
		hasDTZ := rec.Data.DTZ != 0
		if hasCP {
			stats.cpPositions++
		}
		if hasDTM {
			stats.dtmPositions++
		}
		if hasDTZ {
			stats.dtzPositions++
		}
		if hasCP || hasDTM || hasDTZ {
			stats.evalPositions++
		}
	}

	return stats
}

// PositionCounts holds the results of counting positions in a block
type PositionCounts struct {
	Total     uint64
	Evaluated uint64 // Any evaluation (CP, DTM, or DTZ)
	CP        uint64 // Centipawn evaluation
	DTM       uint64 // Distance to mate
	DTZ       uint64 // Distance to zeroing
}

// LoadBlockFilePublic is a public wrapper for loading block files (for export tool)
func (ps *PositionStore) LoadBlockFilePublic(filename string) ([]BlockRecord, error) {
	return ps.loadBlockFile(filename)
}

