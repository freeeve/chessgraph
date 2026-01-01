package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/freeeve/pgn/v2"

	"github.com/freeeve/chessgraph/api/internal/store"
)

func TestPositionStoreBuilderBasic(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	builder := store.NewPositionStoreBuilder(ps)
	builder.SetLogger(t.Logf)

	// Ingest a single position
	startPos := pgn.NewStartingPosition()
	record := &store.PositionRecord{
		Wins:   100,
		Draws:  50,
		Losses: 30,
		CP:     25,
	}

	if err := builder.IngestPosition(startPos, record); err != nil {
		t.Fatal(err)
	}

	if err := builder.Flush(); err != nil {
		t.Fatal(err)
	}

	// Lookup the position
	found, err := builder.LookupPosition(startPos)
	if err != nil {
		t.Fatalf("LookupPosition: %v", err)
	}

	if found.Wins != 100 || found.Draws != 50 || found.Losses != 30 {
		t.Errorf("Record mismatch: got W=%d D=%d L=%d", found.Wins, found.Draws, found.Losses)
	}

	stats := builder.Stats()
	if stats.PositionsStored != 1 {
		t.Errorf("PositionsStored: got %d, want 1", stats.PositionsStored)
	}
}

func TestPositionStoreBuilderEnumeration(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	builder := store.NewPositionStoreBuilder(ps)
	builder.SetLogger(t.Logf)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	startPos := pgn.NewStartingPosition()

	// Build with depth 2 (should be fast, ~400 positions)
	if err := builder.BuildFromEnumeration(ctx, startPos, 2); err != nil {
		t.Fatal(err)
	}

	stats := builder.Stats()
	t.Logf("Enumerated %d positions, stored %d", stats.PositionsEnumerated, stats.PositionsStored)

	// Should have enumerated a reasonable number of positions
	if stats.PositionsEnumerated < 100 {
		t.Errorf("Expected at least 100 positions, got %d", stats.PositionsEnumerated)
	}

	// Verify we can look up the starting position
	found, err := builder.LookupPosition(startPos)
	if err != nil {
		t.Fatalf("LookupPosition (start): %v", err)
	}
	if found == nil {
		t.Error("Starting position not found")
	}

	// Verify we can look up a position after e4
	afterE4, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1")
	found, err = builder.LookupPosition(afterE4)
	if err != nil {
		t.Fatalf("LookupPosition (e4): %v", err)
	}
	if found == nil {
		t.Error("Position after e4 not found")
	}
}

func TestPositionStoreBuilderParallel(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 64)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	builder := store.NewPositionStoreBuilder(ps)
	builder.SetLogger(t.Logf)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	startPos := pgn.NewStartingPosition()

	// Build with depth 2 using parallel enumeration
	if err := builder.BuildFromEnumerationParallel(ctx, startPos, 2); err != nil {
		t.Fatal(err)
	}

	stats := builder.Stats()
	t.Logf("Parallel enumerated %d positions, stored %d", stats.PositionsEnumerated, stats.PositionsStored)

	// Should have same number as sequential
	if stats.PositionsEnumerated < 100 {
		t.Errorf("Expected at least 100 positions, got %d", stats.PositionsEnumerated)
	}
}

func TestPositionStoreBuilderIngestBatch(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	builder := store.NewPositionStoreBuilder(ps)

	// Create batch of positions
	fens := []string{
		"rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
		"rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
		"rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq e6 0 2",
	}

	batch := make([]store.PositionWithRecord, len(fens))
	for i, fen := range fens {
		gs, err := pgn.NewGame(fen)
		if err != nil {
			t.Fatalf("Parse FEN %d: %v", i, err)
		}
		batch[i] = store.PositionWithRecord{
			Key: gs.Pack(),
			Record: store.PositionRecord{
				Wins: uint16(i * 10),
			},
		}
	}

	if err := builder.IngestBatch(batch); err != nil {
		t.Fatal(err)
	}

	if err := builder.Flush(); err != nil {
		t.Fatal(err)
	}

	// Verify all positions
	for i, fen := range fens {
		gs, _ := pgn.NewGame(fen)
		found, err := builder.LookupPosition(gs)
		if err != nil {
			t.Fatalf("Lookup position %d: %v", i, err)
		}
		if found.Wins != uint16(i*10) {
			t.Errorf("Position %d: got Wins=%d, want %d", i, found.Wins, i*10)
		}
	}

	stats := builder.Stats()
	if stats.PositionsStored != 3 {
		t.Errorf("PositionsStored: got %d, want 3", stats.PositionsStored)
	}
}

func TestPositionStoreQuery(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	builder := store.NewPositionStoreBuilder(ps)

	// Add a position
	startPos := pgn.NewStartingPosition()
	record := &store.PositionRecord{Wins: 42}
	builder.IngestPosition(startPos, record)
	builder.Flush()

	// Create query interface
	query := store.NewPositionStoreQuery(ps)

	// Test GetByGameState
	found, err := query.GetByGameState(startPos)
	if err != nil {
		t.Fatalf("GetByGameState: %v", err)
	}
	if found.Wins != 42 {
		t.Errorf("Wins: got %d, want 42", found.Wins)
	}

	// Test GetByFEN
	found, err = query.GetByFEN("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatalf("GetByFEN: %v", err)
	}
	if found.Wins != 42 {
		t.Errorf("Wins: got %d, want 42", found.Wins)
	}

	// Test Exists
	if !query.Exists(startPos.Pack()) {
		t.Error("Exists should return true for starting position")
	}

	// Test non-existent position
	other, _ := pgn.NewGame("8/8/8/8/8/8/8/4K2k w - - 0 1")
	if query.Exists(other.Pack()) {
		t.Error("Exists should return false for non-existent position")
	}
}

func TestPositionStoreBuilderUpdate(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 32)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	builder := store.NewPositionStoreBuilder(ps)

	startPos := pgn.NewStartingPosition()

	// Initial record
	record1 := &store.PositionRecord{Wins: 10, Draws: 5, Losses: 3}
	builder.IngestPosition(startPos, record1)
	builder.Flush()

	// Update record
	record2 := &store.PositionRecord{Wins: 20, Draws: 10, Losses: 6}
	builder.UpdatePosition(startPos, record2)
	builder.Flush()

	// Verify updated
	found, err := builder.LookupPosition(startPos)
	if err != nil {
		t.Fatal(err)
	}
	if found.Wins != 20 {
		t.Errorf("Wins after update: got %d, want 20", found.Wins)
	}
}

func BenchmarkPositionStoreBuilderIngest(b *testing.B) {
	tmpDir := b.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 128)
	if err != nil {
		b.Fatal(err)
	}
	defer ps.Close()

	builder := store.NewPositionStoreBuilder(ps)

	startPos := pgn.NewStartingPosition()
	record := &store.PositionRecord{Wins: 100}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.IngestPosition(startPos, record)
	}
}

func BenchmarkPositionStoreBuilderLookup(b *testing.B) {
	tmpDir := b.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 128)
	if err != nil {
		b.Fatal(err)
	}
	defer ps.Close()

	builder := store.NewPositionStoreBuilder(ps)

	// Add some positions
	startPos := pgn.NewStartingPosition()
	record := &store.PositionRecord{Wins: 100}
	builder.IngestPosition(startPos, record)
	builder.Flush()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		builder.LookupPosition(startPos)
	}
}
