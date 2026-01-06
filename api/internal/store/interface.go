package store

import (
	"errors"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// ErrNotFound is returned when a key is not found in the store.
var ErrNotFound = errors.New("not found")

// ErrPSKeyNotFound is an alias for ErrNotFound (backwards compatibility)
var ErrPSKeyNotFound = ErrNotFound

// PSStats holds statistics about the position store.
type PSStats struct {
	TotalReads         uint64
	TotalWrites        uint64
	DirtyFiles         int    // L0 file count
	DirtyBytes         int64  // memtable size
	CachedBlocks       int    // cached L0 files
	TotalPositions     uint64
	EvaluatedPositions uint64
	CPPositions        uint64
	DTMPositions       uint64
	DTZPositions       uint64
	TotalGames         uint64
	TotalFolders       uint64 // L1 file count
	TotalBlocks        uint64 // total files (L0 + L1)
	UncompressedBytes  uint64
	CompressedBytes    uint64
}

// ReadStore is a read-only interface for position stores.
// Used by HTTP handlers and query operations.
type ReadStore interface {
	Get(key graph.PositionKey) (*PositionRecord, error)
	Stats() PSStats
	IsReadOnly() bool
}

// WriteStore extends ReadStore with write capabilities.
// Used by eval workers and other components that update positions.
type WriteStore interface {
	ReadStore
	Put(key graph.PositionKey, rec *PositionRecord) error
	FlushAll() error
	FlushAllAsync()
	FlushIfMemoryNeededAsync(thresholdBytes int64) bool
}
