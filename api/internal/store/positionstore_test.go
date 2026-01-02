package store_test

import (
	"testing"

	"github.com/freeeve/chessgraph/api/internal/graph"
	"github.com/freeeve/chessgraph/api/internal/store"
	"github.com/freeeve/pgn/v2"
)

func TestExtractPrefix(t *testing.T) {
	// Starting position
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}

	pos := gs.Pack()
	prefix := store.ExtractPrefix(pos)

	// White king on e1 (square 4)
	if prefix.WKingSq != 4 {
		t.Errorf("WKingSq: got %d, want 4", prefix.WKingSq)
	}

	// Black king on e8 (square 60)
	if prefix.BKingSq != 60 {
		t.Errorf("BKingSq: got %d, want 60", prefix.BKingSq)
	}

	// White pawns on a2-h2 (squares 8-15)
	expectedWPawns := uint64(0xFF00)
	if prefix.WPawnsBB != expectedWPawns {
		t.Errorf("WPawnsBB: got %016x, want %016x", prefix.WPawnsBB, expectedWPawns)
	}

	// Black pawns on a7-h7 (squares 48-55)
	expectedBPawns := uint64(0xFF000000000000)
	if prefix.BPawnsBB != expectedBPawns {
		t.Errorf("BPawnsBB: got %016x, want %016x", prefix.BPawnsBB, expectedBPawns)
	}

	// Castle rights: all (0xF)
	if prefix.Castle != 0xF {
		t.Errorf("Castle: got %x, want F", prefix.Castle)
	}

	// No EP
	if prefix.EP != 0xFF {
		t.Errorf("EP: got %x, want FF", prefix.EP)
	}
}

func TestExtractPrefixAfterE4(t *testing.T) {
	// After 1.e4
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1")
	if err != nil {
		t.Fatal(err)
	}

	pos := gs.Pack()
	prefix := store.ExtractPrefix(pos)

	// White pawn moved from e2 to e4
	// Pawns on a2,b2,c2,d2,f2,g2,h2,e4 (squares 8,9,10,11,13,14,15,28)
	expectedWPawns := uint64((1 << 8) | (1 << 9) | (1 << 10) | (1 << 11) |
		(1 << 13) | (1 << 14) | (1 << 15) | (1 << 28))
	if prefix.WPawnsBB != expectedWPawns {
		t.Errorf("WPawnsBB: got %016x, want %016x", prefix.WPawnsBB, expectedWPawns)
	}

	// EP file: e = 4
	if prefix.EP != 4 {
		t.Errorf("EP: got %d, want 4", prefix.EP)
	}
}

func TestExtractSuffix(t *testing.T) {
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}

	pos := gs.Pack()
	suffix := store.ExtractSuffix(pos)

	// V7: Suffix is just pieces (20 bytes), STM is in prefix now
	if len(suffix) != store.SuffixSize {
		t.Errorf("Suffix length: got %d, want %d", len(suffix), store.SuffixSize)
	}

	// The suffix contains packed pieces (N, B, R, Q for both sides)
	// In starting position: 2 WN, 2 WB, 2 WR, 1 WQ, 2 BN, 2 BB, 2 BR, 1 BQ = 14 pieces
	// Just verify the suffix is non-zero (pieces are packed)
	nonZeroBytes := 0
	for i := 0; i < len(suffix); i++ {
		if suffix[i] != 0 {
			nonZeroBytes++
		}
	}
	if nonZeroBytes == 0 {
		t.Error("Suffix should have packed piece data")
	}
	t.Logf("Non-zero suffix bytes: %d", nonZeroBytes)
}

func TestExtractSuffixBlackToMove(t *testing.T) {
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1")
	if err != nil {
		t.Fatal(err)
	}

	pos := gs.Pack()
	suffix := store.ExtractSuffix(pos)
	prefix := store.ExtractPrefix(pos)

	// V7: STM is now in prefix, not suffix
	if prefix.STM != 1 {
		t.Errorf("STM in prefix: got %d, want 1 (black)", prefix.STM)
	}

	// Suffix is just pieces (20 bytes)
	if len(suffix) != store.SuffixSize {
		t.Errorf("Suffix length: got %d, want %d", len(suffix), store.SuffixSize)
	}
}

func TestPrefixPath(t *testing.T) {
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}

	pos := gs.Pack()
	prefix := store.ExtractPrefix(pos)

	folder := prefix.FolderName()
	// WK on e1 (4), BK on e8 (60) -> "043c"
	if folder != "043c" {
		t.Errorf("FolderName: got %s, want 043c", folder)
	}

	filename := prefix.FileName()
	t.Logf("Filename: %s", filename)

	path := prefix.Path()
	t.Logf("Full path: %s", path)
}

func TestPositionStoreBasic(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	// Create a test position
	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}
	pos := gs.Pack()

	// Store a record
	record := &store.PositionRecord{
		Wins:   100,
		Draws:  50,
		Losses: 30,
		CP:     25,
	}

	if err := ps.Put(pos, record); err != nil {
		t.Fatal(err)
	}

	// Should find it in dirty buffer before flush
	found, err := ps.Get(pos)
	if err != nil {
		t.Fatalf("Get before flush: %v", err)
	}
	if found.Wins != 100 {
		t.Errorf("Wins: got %d, want 100", found.Wins)
	}

	// Flush
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Should still find it after flush
	found, err = ps.Get(pos)
	if err != nil {
		t.Fatalf("Get after flush: %v", err)
	}
	if found.Wins != 100 || found.Draws != 50 || found.Losses != 30 {
		t.Errorf("Record mismatch: got W=%d D=%d L=%d", found.Wins, found.Draws, found.Losses)
	}
}

func TestPositionStoreNotFound(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}
	pos := gs.Pack()

	_, err = ps.Get(pos)
	if err != store.ErrPSKeyNotFound {
		t.Errorf("Expected ErrPSKeyNotFound, got %v", err)
	}
}

func TestPositionStoreMultiplePositions(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	// Store positions from a short game
	fens := []string{
		"rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
		"rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
		"rnbqkbnr/pppp1ppp/8/4p3/4P3/8/PPPP1PPP/RNBQKBNR w KQkq e6 0 2",
		"rnbqkbnr/pppp1ppp/8/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R b KQkq - 1 2",
	}

	positions := make([]graph.PositionKey, len(fens))
	for i, fen := range fens {
		gs, err := pgn.NewGame(fen)
		if err != nil {
			t.Fatalf("Parse FEN %d: %v", i, err)
		}
		positions[i] = gs.Pack()

		record := &store.PositionRecord{
			Wins:   uint16(i * 10),
			Draws:  uint16(i * 5),
			Losses: uint16(i * 3),
		}

		if err := ps.Put(positions[i], record); err != nil {
			t.Fatal(err)
		}
	}

	// Flush
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Verify all
	for i, pos := range positions {
		found, err := ps.Get(pos)
		if err != nil {
			t.Fatalf("Get position %d: %v", i, err)
		}
		if found.Wins != uint16(i*10) {
			t.Errorf("Position %d: got Wins=%d, want %d", i, found.Wins, i*10)
		}
	}

	stats := ps.Stats()
	t.Logf("Stats: reads=%d writes=%d cached=%d", stats.TotalReads, stats.TotalWrites, stats.CachedBlocks)
}

func TestPositionStoreMerge(t *testing.T) {
	tmpDir := t.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()

	gs, err := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	if err != nil {
		t.Fatal(err)
	}
	pos := gs.Pack()

	// First write
	record1 := &store.PositionRecord{Wins: 100, Draws: 50, Losses: 30}
	if err := ps.Put(pos, record1); err != nil {
		t.Fatal(err)
	}
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Second write (update)
	record2 := &store.PositionRecord{Wins: 200, Draws: 100, Losses: 60}
	if err := ps.Put(pos, record2); err != nil {
		t.Fatal(err)
	}
	if err := ps.FlushAll(); err != nil {
		t.Fatal(err)
	}

	// Should have merged value
	found, err := ps.Get(pos)
	if err != nil {
		t.Fatal(err)
	}
	if found.Wins != 200 {
		t.Errorf("After merge: got Wins=%d, want 200", found.Wins)
	}
}

func TestReconstructKey(t *testing.T) {
	// Test that we can reconstruct the original key from prefix+suffix
	fens := []string{
		"rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1",
		"rnbqkbnr/pppppppp/8/8/4P3/8/PPPP1PPP/RNBQKBNR b KQkq e3 0 1",
		"r1bqkb1r/pppp1ppp/2n2n2/4p3/4P3/5N2/PPPP1PPP/RNBQKB1R w KQkq - 2 3",
		"8/8/8/8/8/8/8/4K2k w - - 0 1", // Minimal endgame
	}

	for _, fen := range fens {
		gs, err := pgn.NewGame(fen)
		if err != nil {
			t.Fatalf("Parse FEN %s: %v", fen, err)
		}
		original := gs.Pack()

		prefix := store.ExtractPrefix(original)
		suffix := store.ExtractSuffix(original)
		reconstructed := store.ReconstructKey(prefix, suffix)

		if original != reconstructed {
			t.Errorf("Reconstruction failed for %s", fen)
			t.Errorf("  Original:      %x", original)
			t.Errorf("  Reconstructed: %x", reconstructed)
		}
	}
}

func BenchmarkExtractPrefix(b *testing.B) {
	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.ExtractPrefix(pos)
	}
}

func BenchmarkExtractSuffix(b *testing.B) {
	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.ExtractSuffix(pos)
	}
}

func BenchmarkPositionStorePut(b *testing.B) {
	tmpDir := b.TempDir()

	ps, err := store.NewPositionStore(tmpDir, 64)
	if err != nil {
		b.Fatal(err)
	}
	defer ps.Close()

	gs, _ := pgn.NewGame("rnbqkbnr/pppppppp/8/8/8/8/PPPPPPPP/RNBQKBNR w KQkq - 0 1")
	pos := gs.Pack()
	record := &store.PositionRecord{Wins: 100}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ps.Put(pos, record)
	}
}
