package store

import (
	"errors"
	"time"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// ErrNotFound is returned when a key is not found in the store.
var ErrNotFound = errors.New("not found")

// ErrPSKeyNotFound is an alias for ErrNotFound (backwards compatibility)
var ErrPSKeyNotFound = ErrNotFound

// Stats holds statistics about the position store.
type Stats struct {
	TotalReads         uint64
	TotalWrites        uint64
	TotalPositions     uint64
	EvaluatedPositions uint64
	CPPositions        uint64
	DTMPositions       uint64
	DTZPositions       uint64
	TotalGames         uint64

	// Memtable stats
	MemtablePositions uint64
	MemtableBytes     int64

	// L0 stats
	L0Files             int
	L0Positions         uint64
	L0UncompressedBytes uint64
	L0CompressedBytes   uint64
	L0CachedFiles       int

	// L1 stats
	L1Files             int
	L1Positions         uint64
	L1UncompressedBytes uint64
	L1CompressedBytes   uint64
	L1CachedFiles       int

	// L2 stats
	L2Blocks            int
	L2Files             int    // Same as L2Blocks (backwards compatibility)
	L2Positions         uint64
	L2UncompressedBytes uint64
	L2CompressedBytes   uint64
	L2CachedFiles       int    // Currently unused

	// Legacy fields for backwards compatibility
	DirtyFiles        int    // L0 file count (deprecated, use L0Files)
	DirtyBytes        int64  // memtable size (deprecated, use MemtableBytes)
	CachedBlocks      int    // cached L0 files (deprecated, use L0CachedFiles)
	TotalFolders      uint64 // L1 file count (deprecated, use L1Files)
	TotalBlocks       uint64 // total files (L0 + L1)
	UncompressedBytes uint64 // total uncompressed (deprecated)
	CompressedBytes   uint64 // total compressed (deprecated)
}

// GetTiming provides timing breakdown for Get operations
type GetTiming struct {
	Memtable   time.Duration
	L0         time.Duration
	L1         time.Duration
	L2         time.Duration
	PosCache   time.Duration
	Total      time.Duration
	CacheHit   bool // true if position cache was hit
	L0CacheHit bool // true if L0 file was in cache
	L1CacheHit bool // true if L1 file was in cache
	L2CacheHit bool // true if L2 file was in cache

	// Additional fields
	Found       bool
	Source      string // "memtable", "l0", "l1", "l2", "cache"
	L0Checked   int
	L1Checked   int
	L2Checked   int
	TotalTimeNs int64
}

// ReadStore is a read-only interface for position stores.
type ReadStore interface {
	Get(key graph.PositionKey) (*PositionRecord, error)
	Stats() Stats
	IsReadOnly() bool
}

// TimedStore optionally provides timing information for Get operations
type TimedStore interface {
	GetWithTiming(key graph.PositionKey) (*PositionRecord, GetTiming)
	L0FileCount() int
}

// WriteStore extends ReadStore with write capabilities.
type WriteStore interface {
	ReadStore
	Put(key graph.PositionKey, rec *PositionRecord) error
	Increment(key graph.PositionKey, wins, draws, losses uint16)
	FlushAll() error
	FlushAllAsync()
	FlushIfMemoryNeededAsync(thresholdBytes int64) bool
}

// IngestStore extends WriteStore with ingestion-specific methods
type IngestStore interface {
	WriteStore
	IncrementWorker(workerID int, key [KeySize]byte, wins, draws, losses uint16)
	FlushWorkerIfNeeded(workerID int) error
	IncrementGameCount(n uint64)
	NumWorkers() int
	SetLogger(f func(format string, args ...any))
	CompactL0() error
	CompactL1ToL2() error
	StartBackgroundCompaction(interval time.Duration)
	StopBackgroundCompaction()
}

// PositionFile is the interface for reading sorted segment files
type PositionFile interface {
	Get(key [KeySize]byte) (*PositionRecord, error)
	RecordCount() int
	MinKey() [KeySize]byte
	MaxKey() [KeySize]byte
	Iterator() PositionIterator
}

// PositionIterator iterates over records in a position file
type PositionIterator interface {
	Next() *Record
}

// RecordIterator is an interface for iterating over sorted Records
type RecordIterator interface {
	// Next returns the next record, or nil if exhausted
	Next() *Record
	// Peek returns the next record without consuming it, or nil if exhausted
	Peek() *Record
}
