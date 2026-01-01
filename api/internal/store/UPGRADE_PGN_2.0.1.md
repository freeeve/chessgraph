# Upgrading to pgn v2.0.1

The tablebase implementation is ready for the pgn v2.0.1 DFS enumeration API. Once pgn v2.0.1 is released, follow these steps:

## 1. Update pgn dependency

```bash
go get github.com/freeeve/pgn/v2@v2.0.1
go mod tidy
```

## 2. Update `internal/store/tablebase_builder.go`

### Replace placeholder types (lines 17-37)

**Remove:**
```go
// Type aliases for pgn DFS enumeration API (requires v2.0.1+)
// Commented out until pgn v2.0.1 is released:
// type PositionEnumerator = *pgn.PositionEnumeratorDFS
// type EnumeratorCheckpoint = pgn.CheckpointDFS

// Temporary placeholder until pgn v2.0.1 is available
type PositionEnumerator interface {
	EnumerateDFS(maxDepth int, callback func(uint64, *pgn.GameState, int) bool)
	EnumerateDFSParallel(maxDepth int, callback func(uint64, *pgn.GameState, int) bool)
	PositionAtIndexDFS(idx uint64, maxDepth int) (*pgn.GameState, bool)
	IndexOfPositionDFS(target *pgn.GameState, maxDepth int) (uint64, bool)
	SaveCheckpointsCSV(filename string, maxDepth int) error
	LoadCheckpointsCSV(filename string) (int, error)
	GetCheckpointsDFS() []*EnumeratorCheckpoint
}

type EnumeratorCheckpoint struct {
	Index uint64
	Depth int
	State *pgn.GameState
}
```

**Replace with:**
```go
// Type aliases for pgn DFS enumeration API (v2.0.1+)
type PositionEnumerator = *pgn.PositionEnumeratorDFS
type EnumeratorCheckpoint = pgn.CheckpointDFS
```

### Update NewTablebaseBuilder (line 47)

**Change:**
```go
return &TablebaseBuilder{
	tb: tb,
	// Uncomment when pgn v2.0.1 is available:
	// enum: pgn.NewPositionEnumeratorDFS(startPos),
	enum: nil, // TODO: Set when pgn v2.0.1 is released
}
```

**To:**
```go
return &TablebaseBuilder{
	tb:   tb,
	enum: pgn.NewPositionEnumeratorDFS(startPos),
}
```

### Update NewTablebaseQuery (line 281)

**Change:**
```go
return &TablebaseQuery{
	tb: tb,
	// Uncomment when pgn v2.0.1 is available:
	// enum: pgn.NewPositionEnumeratorDFS(startPos),
	enum: nil, // TODO: Set when pgn v2.0.1 is released
}
```

**To:**
```go
return &TablebaseQuery{
	tb:   tb,
	enum: pgn.NewPositionEnumeratorDFS(startPos),
}
```

## 3. Verify compilation

```bash
go build ./internal/store/...
go test ./internal/store/... -run TestTablebase
```

## 4. Test the integration

```go
package main

import (
	"context"
	"log"

	"github.com/freeeve/pgn/v2"
	"github.com/freeeve/chessgraph/api/internal/store"
)

func main() {
	// Create tablebase
	tb, err := store.NewTablebase("./test_tb", 16)
	if err != nil {
		log.Fatal(err)
	}
	defer tb.Close()

	// Create builder with standard starting position
	builder := store.NewTablebaseBuilder(tb, pgn.NewStartingPosition())

	// Build depth 0-5 using DFS
	ctx := context.Background()
	err = builder.BuildFromEnumeration(ctx, 5)
	if err != nil {
		log.Fatal(err)
	}

	// Or use parallel version (~4x faster)
	// err = builder.BuildFromEnumerationParallel(ctx, 5)

	stats := tb.Stats()
	log.Printf("Built tablebase: %d positions across %d blocks\n",
		stats.TotalRecords, stats.BlockCount)

	// Test position lookup
	pos, record, err := builder.GetPositionByIndex(42, 5)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Position 42: %s\n", pos.ToFEN())
	log.Printf("Record: W=%d D=%d L=%d\n", record.Wins, record.Draws, record.Losses)

	// Test index lookup
	index, err := builder.GetIndexOfPosition(pos, 5)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Position has index: %d\n", index)

	// Save checkpoints
	err = builder.SaveCheckpoints("checkpoints.csv", 5)
	if err != nil {
		log.Fatal(err)
	}
}
```

## Quick sed commands for bulk updates

If you prefer automated updates:

```bash
# In internal/store/tablebase_builder.go

# Remove placeholder interface and replace with type aliases
sed -i '/^\/\/ Temporary placeholder/,/^}/d' tablebase_builder.go
sed -i 's|// type PositionEnumerator = \*pgn.PositionEnumeratorDFS|type PositionEnumerator = *pgn.PositionEnumeratorDFS|' tablebase_builder.go
sed -i 's|// type EnumeratorCheckpoint = pgn.CheckpointDFS|type EnumeratorCheckpoint = pgn.CheckpointDFS|' tablebase_builder.go

# Update NewTablebaseBuilder
sed -i 's|enum: nil, // TODO: Set when pgn v2.0.1 is released|enum: pgn.NewPositionEnumeratorDFS(startPos),|' tablebase_builder.go

# Remove TODO comments
sed -i '/\/\/ Uncomment when pgn v2.0.1 is available:/d' tablebase_builder.go
sed -i 's|// enum: pgn.NewPositionEnumeratorDFS(startPos),||' tablebase_builder.go
```

## What's New in v2.0.1

The pgn v2.0.1 DFS enumeration API provides:

- **Deterministic DFS ordering**: Positions are indexed consistently
- **Parallel enumeration**: ~4x faster with `EnumerateDFSParallel`
- **Bidirectional mapping**:
  - `PositionAtIndexDFS`: Get position from index (~50-150ms)
  - `IndexOfPositionDFS`: Get index from position
- **Checkpoint support**: Save/load enumeration state for large searches
- **Metadata**: CSV checkpoints include depth information

This enables:
- ✅ Iterative deepening with consistent indexes
- ✅ Fast position lookups in tablebases
- ✅ Resumable large-scale enumerations
- ✅ Position-to-index mapping for game analysis
