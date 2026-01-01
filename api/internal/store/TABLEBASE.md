# Tablebase Storage System

This implements a Zyzygy-style tablebase storage system for chess positions using compressed blocks and efficient indexing.

## Architecture

### Block-Based Storage
- Positions are stored in blocks of **2^20 (1,048,576)** positions each
- Each block contains fixed-size 14-byte `PositionRecord` structs
- Blocks are compressed using **zstd** for efficient storage
- Only used positions are tracked via **RoaringBitmap** to save space

### LRU Cache
- Decompressed blocks are cached in memory (default: 16 blocks)
- Cache uses LRU (Least Recently Used) eviction policy
- Configurable cache size based on memory constraints

### Metadata
- Block metadata tracks:
  - Block ID and key position
  - Live position bitmap (which positions in the block are actually used)
  - Record count and compressed size
- Metadata persisted to disk in binary format

## Components

### Core Types

#### `Tablebase`
Main storage engine that manages blocks, compression, and caching.

```go
tb, err := store.NewTablebase("/path/to/tablebase", 16) // 16 cached blocks
if err != nil {
    return err
}
defer tb.Close()

// Get a position by index
record, err := tb.GetPosition(42)

// Put a position
record := &store.PositionRecord{
    Wins: 10,
    Draws: 5,
    Losses: 2,
    CP: 150,
    DTM: store.DTMUnknown,
}
err = tb.PutPosition(42, record)

// Flush to disk
err = tb.Flush()
```

#### `TablebaseBuilder`
Helper for building tablebases using position enumeration (requires pgn API update).

```go
builder := store.NewTablebaseBuilder(tb, nil) // nil = standard starting position

// Build from enumeration (when pgn API is ready)
ctx := context.Background()
err := builder.BuildFromEnumeration(ctx, 0, 5) // depth 0-5

// Get position by index
pos, record, err := builder.GetPositionByIndex(1000)

// Update a position
record.Wins = 100
err = builder.UpdatePosition(1000, record)
```

#### `BatchWriter`
Efficient batch writing with automatic flushing.

```go
writer := store.NewBatchWriter(tb, 10000) // batch size = 10000

for i := 0; i < 1000000; i++ {
    record := store.PositionRecord{
        Wins: uint16(i % 100),
        // ...
    }
    err := writer.Add(uint64(i), record)
    if err != nil {
        return err
    }
}

// Flush remaining
err := writer.Flush()
```

#### `TablebaseQuery`
Query interface for retrieving positions.

```go
query := store.NewTablebaseQuery(tb, nil)

// Get position record
record, err := query.GetPosition(index)

// Get position with game state (when pgn API is ready)
pos, record, err := query.GetPositionWithGameState(index)

// Get statistics
stats := query.Stats()
fmt.Printf("Blocks: %d, Records: %d, Cached: %d\n",
    stats.BlockCount, stats.TotalRecords, stats.CachedBlocks)
```

## Position Record Format

Each position is stored as a 14-byte record:

```go
type PositionRecord struct {
    Wins        uint16 // Win count (caps at 65535)
    Draws       uint16 // Draw count (caps at 65535)
    Losses      uint16 // Loss count (caps at 65535)
    CP          int16  // Centipawn eval (Stockfish)
    DTM         int16  // Depth To Mate
    DTZ         uint16 // Depth To Zeroing (reserved)
    ProvenDepth uint16 // Propagated proven depth
}
```

### DTM Encoding
- `DTMUnknown` (0): Unknown/not analyzed
- Positive values (1-16384): We deliver mate in N moves
- Negative values (-1 to -16384): We get mated in N moves
- Values ≤ -16385: Draw at distance (N + 32768)
- Special sentinels:
  - `DTMMate0White` (32767): White is checkmated
  - `DTMMate0Black` (32766): Black is checkmated

## Storage Layout

```
tablebase/
├── tablebase.meta          # Metadata file
├── block_0.dat            # Compressed block 0 (positions 0-1048575)
├── block_1.dat            # Compressed block 1 (positions 1048576-2097151)
└── block_N.dat            # Compressed block N
```

### Metadata Format
```
Version (4 bytes)
BlockCount (8 bytes)
TotalRecords (8 bytes)
For each block:
  BlockID (8 bytes)
  KeyPosition (34 bytes)
  RecordCount (8 bytes)
  CompressedSize (8 bytes)
  LiveBitmapSize (4 bytes)
  LiveBitmap (variable bytes)
```

## Integration with Position Enumerator

**Note:** The position enumerator API is currently being developed in the pgn library. Once available, it will provide:

```go
// Create enumerator from starting position
enum := pgn.NewPositionEnumerator(nil) // nil = standard start

// Enumerate positions depth 0-3
enum.EnumerateBFS(0, 3, func(index uint64, pos *pgn.GameState, depth int) {
    // Store position in tablebase
    record := store.PositionRecord{...}
    tb.PutPosition(index, &record)
})

// Get checkpoint for resuming
ckpt := enum.GetCheckpointForIndex(1000000)

// Restart from checkpoint
enum.RestartFromCheckpoint(0, 10, callback)

// Lookup position by index
pos, _ := enum.PositionAtIndex(42)
```

## Performance Considerations

### Memory Usage
- Each cached block: ~14 MB decompressed (1,048,576 * 14 bytes)
- Default 16 blocks cached: ~224 MB
- Adjust cache size based on available memory:
  ```go
  tb, err := store.NewTablebase(dir, 32) // 32 blocks = ~448 MB
  ```

### Compression
- zstd compression typically achieves 70-90% compression ratio
- Balance between compression level and speed (currently using default)
- Can be tuned by modifying `zstd.EncoderLevel` in tablebase.go

### Disk I/O
- Sequential writes are batched for efficiency
- Periodic flushing (every 10 batches by default)
- Read operations benefit from LRU cache
- Consider SSD for better random access performance

## Error Handling

```go
// Check for specific errors
record, err := tb.GetPosition(index)
if errors.Is(err, store.ErrBlockNotFound) {
    // Block hasn't been created yet
}
if errors.Is(err, store.ErrIndexOutOfRange) {
    // Index is beyond valid range
}
```

## Iterative Deepening

Since position indexes change with different search depths and strategies, the tablebase supports iterative deepening with snapshots:

### Basic Workflow

```go
// Create iterative deepening manager
id, err := store.NewIterativeDeepening("./data/tablebase", 16)
if err != nil {
    return err
}
defer id.Close()

// Build depth 0-2
tb, err := id.StartDepth(2, false) // false = don't resume
// ... build positions ...
tb.Flush()
tb.Close()

// Build depth 0-3, copying from depth 2
tb, err = id.StartDepth(3, true) // true = resume from previous depth
// ... add new positions at depth 3 ...
tb.Flush()

// Save snapshot
err = id.SaveSnapshot("depth_3_complete")
tb.Close()

// Later: load specific snapshot
tbSnapshot, err := id.LoadSnapshot("depth_3_complete")
```

### Copy Operations

```go
// Copy entire tablebase to new directory
err := tb.CopyTo("/path/to/backup")

// Clone a tablebase
newTb, err := tb.Clone("/path/to/clone")
```

### Merging Tablebases

When you have multiple tablebases (e.g., from different analyses), merge them:

```go
// Merge tb2 into tb1 using default strategy
err := tb1.MergeFrom(tb2, store.DefaultMergeStrategy)

// Custom merge strategy
err := tb1.MergeFrom(tb2, func(index uint64, existing, incoming *store.PositionRecord) *store.PositionRecord {
    // Custom logic: prefer records with more games
    if existing.Count() > incoming.Count() {
        return existing
    }
    return incoming
})
```

**DefaultMergeStrategy** combines:
- Game stats (Wins/Draws/Losses) by addition
- CP eval by taking maximum
- DTM by preferring wins > draws > losses
- ProvenDepth by taking maximum

### Index Remapping

When enumeration order changes (BFS→DFS, different depths), remap positions:

```go
// Export all positions
err := src.ExportPositions(func(index uint64, record *store.PositionRecord) error {
    // Process each position
    return nil
})

// Import with index mapping
err := dst.ImportPositions(src, func(oldIndex uint64) (newIndex uint64, include bool) {
    // Map old index to new index
    // Return include=false to skip this position
    return oldIndex + 1000, true
})
```

## Directory Structure for Iterative Deepening

```
data/tablebase/
├── depth_1/                 # Depth 0-1
│   ├── tablebase.meta
│   └── block_*.dat
├── depth_2/                 # Depth 0-2 (copied from depth_1 + new positions)
│   ├── tablebase.meta
│   └── block_*.dat
├── depth_3/                 # Depth 0-3
│   └── ...
└── snapshot_milestone/      # Named snapshots
    └── ...
```

## Future Enhancements

- [ ] Parallel block compression/decompression
- [ ] Memory-mapped I/O for faster access
- [ ] Delta encoding for related positions
- [ ] Incremental metadata saves
- [ ] Background compression of dirty blocks
- [ ] Support for sparse tablebase files
- [x] Iterative deepening support
- [x] Tablebase copying and merging
- [x] Position index remapping

## Example: Building a Complete Tablebase

```go
package main

import (
    "context"
    "log"

    "github.com/freeeve/chessgraph/api/internal/store"
)

func main() {
    // Create tablebase
    tb, err := store.NewTablebase("./data/tablebase", 16)
    if err != nil {
        log.Fatal(err)
    }
    defer tb.Close()

    // Create batch writer
    writer := store.NewBatchWriter(tb, 10000)

    // Populate with positions (when pgn enumerator is ready)
    // For now, manually add positions:
    for i := uint64(0); i < 5000000; i++ {
        record := store.PositionRecord{
            Wins:   uint16(i % 100),
            Draws:  uint16(i % 50),
            Losses: uint16(i % 25),
            CP:     int16(i % 2000),
            DTM:    store.DTMUnknown,
        }

        if err := writer.Add(i, record); err != nil {
            log.Fatal(err)
        }

        if i%100000 == 0 {
            log.Printf("Processed %d positions", i)
        }
    }

    // Final flush
    if err := writer.Flush(); err != nil {
        log.Fatal(err)
    }
    if err := tb.Flush(); err != nil {
        log.Fatal(err)
    }

    // Print stats
    stats := tb.Stats()
    log.Printf("Complete! Blocks: %d, Records: %d, Compressed: %.2f MB",
        stats.BlockCount,
        stats.TotalRecords,
        float64(stats.CompressedSize)/(1024*1024))
}
```
