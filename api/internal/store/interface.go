package store

import (
	"github.com/freeeve/chessgraph/api/internal/graph"
)

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
