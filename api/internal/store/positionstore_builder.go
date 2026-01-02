package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/freeeve/pgn/v2"

	"github.com/freeeve/chessgraph/api/internal/graph"
)

// PositionStoreBuilder builds a PositionStore by enumerating chess positions
type PositionStoreBuilder struct {
	ps  *PositionStore
	mu  sync.Mutex
	log func(format string, args ...any)

	// Stats
	positionsEnumerated atomic.Uint64
	positionsStored     atomic.Uint64
}

// NewPositionStoreBuilder creates a new position store builder
func NewPositionStoreBuilder(ps *PositionStore) *PositionStoreBuilder {
	return &PositionStoreBuilder{
		ps:  ps,
		log: func(format string, args ...any) {},
	}
}

// SetLogger sets a logging function
func (b *PositionStoreBuilder) SetLogger(log func(format string, args ...any)) {
	b.log = log
}

// BuildFromEnumeration populates the store by enumerating positions using DFS
func (b *PositionStoreBuilder) BuildFromEnumeration(ctx context.Context, startPos *pgn.GameState, maxDepth int) error {
	enum := pgn.NewPositionEnumeratorDFS(startPos)

	var buildErr error

	b.log("starting position store build to depth %d...", maxDepth)

	callback := func(index uint64, pos *pgn.GameState, depth int) bool {
		// Check context cancellation
		select {
		case <-ctx.Done():
			buildErr = ctx.Err()
			return false
		default:
		}

		// Get the packed position key
		packed := pos.Pack()

		// Create initial empty record (HasCP flag is false since ProvenDepth is 0)
		record := PositionRecord{
			Wins:        0,
			Draws:       0,
			Losses:      0,
			CP:          0,
			DTM:         DTMUnknown,
			DTZ:         0,
			ProvenDepth: 0,
		}

		// Store the position
		if err := b.ps.Put(packed, &record); err != nil {
			return false
		}

		count := b.positionsEnumerated.Add(1)
		b.positionsStored.Add(1)

		if count%1000000 == 0 {
			b.log("enumerated %d positions (depth %d)...", count, depth)
		}

		// Flush when we have too many dirty files
		if err := b.ps.FlushIfNeeded(256); err != nil {
			buildErr = err
			return false
		}

		return true
	}

	enum.EnumerateDFS(maxDepth, callback)

	if buildErr != nil {
		return buildErr
	}

	b.log("enumeration complete: %d positions", b.positionsEnumerated.Load())

	return b.ps.FlushAll()
}

// BuildFromEnumerationParallel populates the store using parallel enumeration
func (b *PositionStoreBuilder) BuildFromEnumerationParallel(ctx context.Context, startPos *pgn.GameState, maxDepth int) error {
	enum := pgn.NewPositionEnumeratorDFS(startPos)

	var buildErr error
	var errMu sync.Mutex

	b.log("starting parallel position store build to depth %d...", maxDepth)

	callback := func(index uint64, pos *pgn.GameState, depth int) bool {
		// Check context cancellation
		select {
		case <-ctx.Done():
			errMu.Lock()
			if buildErr == nil {
				buildErr = ctx.Err()
			}
			errMu.Unlock()
			return false
		default:
		}

		// Check if we already have an error
		errMu.Lock()
		hasErr := buildErr != nil
		errMu.Unlock()
		if hasErr {
			return false
		}

		// Get the packed position key
		packed := pos.Pack()

		// Create initial empty record (HasCP flag is false since ProvenDepth is 0)
		record := PositionRecord{
			Wins:        0,
			Draws:       0,
			Losses:      0,
			CP:          0,
			DTM:         DTMUnknown,
			DTZ:         0,
			ProvenDepth: 0,
		}

		// Store the position
		if err := b.ps.Put(packed, &record); err != nil {
			errMu.Lock()
			if buildErr == nil {
				buildErr = err
			}
			errMu.Unlock()
			return false
		}

		count := b.positionsEnumerated.Add(1)
		b.positionsStored.Add(1)

		if count%1000000 == 0 {
			b.log("enumerated %d positions (depth %d)...", count, depth)
		}

		// Flush when we have too many dirty files
		if err := b.ps.FlushIfNeeded(256); err != nil {
			errMu.Lock()
			if buildErr == nil {
				buildErr = err
			}
			errMu.Unlock()
			return false
		}

		return true
	}

	enum.EnumerateDFSParallel(maxDepth, callback)

	if buildErr != nil {
		return buildErr
	}

	b.log("enumeration complete: %d positions", b.positionsEnumerated.Load())

	return b.ps.FlushAll()
}

// IngestPosition adds or updates a single position
func (b *PositionStoreBuilder) IngestPosition(pos *pgn.GameState, record *PositionRecord) error {
	packed := pos.Pack()
	b.positionsStored.Add(1)
	return b.ps.Put(packed, record)
}

// IngestPositionByKey adds or updates a position using its packed key
func (b *PositionStoreBuilder) IngestPositionByKey(key graph.PositionKey, record *PositionRecord) error {
	b.positionsStored.Add(1)
	return b.ps.Put(key, record)
}

// IngestBatch adds multiple positions efficiently
func (b *PositionStoreBuilder) IngestBatch(positions []PositionWithRecord) error {
	for _, p := range positions {
		if err := b.ps.Put(p.Key, &p.Record); err != nil {
			return fmt.Errorf("ingest position: %w", err)
		}
		b.positionsStored.Add(1)
	}
	return nil
}

// PositionWithRecord pairs a position key with its record
type PositionWithRecord struct {
	Key    graph.PositionKey
	Record PositionRecord
}

// Flush writes all pending positions to disk
func (b *PositionStoreBuilder) Flush() error {
	return b.ps.FlushAll()
}

// Stats returns builder statistics
type PSBuilderStats struct {
	PositionsEnumerated uint64
	PositionsStored     uint64
	DirtyFiles          int
	CachedBlocks        int
}

func (b *PositionStoreBuilder) Stats() PSBuilderStats {
	psStats := b.ps.Stats()
	return PSBuilderStats{
		PositionsEnumerated: b.positionsEnumerated.Load(),
		PositionsStored:     b.positionsStored.Load(),
		DirtyFiles:          psStats.DirtyFiles,
		CachedBlocks:        psStats.CachedBlocks,
	}
}

// LookupPosition retrieves a position record by its GameState
func (b *PositionStoreBuilder) LookupPosition(pos *pgn.GameState) (*PositionRecord, error) {
	packed := pos.Pack()
	return b.ps.Get(packed)
}

// LookupByKey retrieves a position record by its packed key
func (b *PositionStoreBuilder) LookupByKey(key graph.PositionKey) (*PositionRecord, error) {
	return b.ps.Get(key)
}

// UpdatePosition updates a position record
func (b *PositionStoreBuilder) UpdatePosition(pos *pgn.GameState, record *PositionRecord) error {
	packed := pos.Pack()
	return b.ps.Put(packed, record)
}

// UpdateByKey updates a position record by its packed key
func (b *PositionStoreBuilder) UpdateByKey(key graph.PositionKey, record *PositionRecord) error {
	return b.ps.Put(key, record)
}

// PositionStoreQuery provides query methods for the position store
type PositionStoreQuery struct {
	ps *PositionStore
}

// NewPositionStoreQuery creates a new query interface
func NewPositionStoreQuery(ps *PositionStore) *PositionStoreQuery {
	return &PositionStoreQuery{ps: ps}
}

// Get retrieves a position record by its packed key
func (q *PositionStoreQuery) Get(key graph.PositionKey) (*PositionRecord, error) {
	return q.ps.Get(key)
}

// GetByGameState retrieves a position record by GameState
func (q *PositionStoreQuery) GetByGameState(pos *pgn.GameState) (*PositionRecord, error) {
	return q.ps.Get(pos.Pack())
}

// GetByFEN retrieves a position record by FEN string
func (q *PositionStoreQuery) GetByFEN(fen string) (*PositionRecord, error) {
	pos, err := pgn.NewGame(fen)
	if err != nil {
		return nil, fmt.Errorf("parse FEN: %w", err)
	}
	return q.ps.Get(pos.Pack())
}

// Exists checks if a position exists in the store
func (q *PositionStoreQuery) Exists(key graph.PositionKey) bool {
	_, err := q.ps.Get(key)
	return err == nil
}

// Stats returns position store statistics
func (q *PositionStoreQuery) Stats() PSStats {
	return q.ps.Stats()
}
